# encoding: utf-8
#
# DO NOT DELETE THE ENCODING LINE ABOVE!!!
# Without it, Ruby assumes ASCII, while our REST API and the SQLite DB
# both assume UTF-8, and everything breaks.

require 'json'
require 'net/http'
require 'openssl'
require 'sqlite3'

# APIImporter imports data from the SQLite database (which is a dump
# of our old Fedora data) into Pharos through the API. In order for
# this to work, you must first import Users, Institutions and Roles
# via script (see the rake task called transition_Fluctus. That rake
# task can import all records, and we'll use it to do that in
# production. But we'll import data from our demo environment through
# the API, making thousands of HTTP calls, to test the API for
# correctness and robustness under load.
class APIImporter

  def initialize(api_key)
    @api_key = api_key
    @db = SQLite3::Database.new("fedora_export.db")
    @db.results_as_hash = true
    @db.execute('PRAGMA encoding = "UTF-8"')
    @new_id_for = {} # Hash: key is old Solr pid, value is new numeric id
    @name_of = {} # Hash: key is Solr pid, value is institution domain name
    # @base_url = 'https://demo.aptrust.org:443'
    @base_url = 'http://localhost:3000'
    @batch_size = 100
    @id_for_name = {}
  end

  # Run the import job. If limit is specified (an integer),
  # this will import only the specified number of objects
  # and work items.
  def run(limit)
    @log = File.open('import.log', 'w')
    create_indexes
    load_institutions
    import_objects(limit)
    import_work_items(limit)
    @log.close
  end

  def create_indexes
    puts "Creating SQLite indexes"
    @db.execute("create index if not exists ix_gf_obj_id " +
                "on generic_files(intellectual_object_id)")
    @db.execute("create index if not exists ix_cs_gf_id " +
                "on checksums(generic_file_id)")
    @db.execute("create index if not exists ix_items_obj_identifier " +
                "on processed_items(object_identifier)")
    @db.execute("create index if not exists ix_event_obj_id " +
                "on premis_events_solr(intellectual_object_id)")
    @db.execute("create index if not exists ix_event_gf_id " +
                "on premis_events_solr(generic_file_id)")
  end

  def import_objects(limit)
    query = "SELECT id, identifier, title, description, alt_identifier, " +
      "access, bag_name, institution_id, state FROM intellectual_objects"
    query += " limit #{limit}" if limit
    if @obj_query.nil?
      @obj_query = @db.prepare(query)
    end
    result_set = @obj_query.execute
    result_set.each_hash do |row|
      pid = row['id'].strip.force_encoding('UTF-8')
      id = import_object(row)
      @new_id_for[pid] = id
      @id_for_name[row['identifier']] = id
      import_files(pid, row['identifier'], id)
      import_object_level_events(pid, row['identifier'])
      puts "Saved object #{row['identifier']} with id #{id}"
    end
  end

  # Send one IntellectualObject record to Pharos through the API.
  # Param row is a row of data selected from the db.
  # Returns the id of the saved IntellectualObject.
  def import_object(row)
    obj = {}
    obj['intellectual_object[identifier]'] = row['identifier']
    obj['intellectual_object[title]'] = row['title']
    obj['intellectual_object[description]'] = row['description']
    obj['intellectual_object[alt_identifier]'] = row['alt_identifier']
    obj['intellectual_object[access]'] = row['access']
    obj['intellectual_object[bag_name]'] = row['bag_name']
    obj['intellectual_object[state]'] = row['state']

    obj['intellectual_object[institution_id]'] = @new_id_for[row['institution_id']]
    obj['intellectual_object[etag]'] = get_etag(row['identifier'])
    obj['intellectual_object[created_at]'] = get_obj_create_time(row['id'])
    obj['intellectual_object[dpn_uuid]'] = get_dpn_uuid(row['id'])

    inst = @name_of[row['institution_id']]
    url = "#{@base_url}/api/v2/objects/#{inst}.json"
    resp = api_post(url, obj)
    if resp.code != '201'
      @log.write("Error saving object #{obj['intellectual_object[identifier]']}\n\n")
      @log.write(resp.body)
      exit(1)
    end
    data = JSON.parse(resp.body)
    return data['id']
  end

  def get_obj_create_time(object_pid)
    timestamp = nil
    query = "select date_time from premis_events_solr " +
      "where intellectual_object_id = ? and event_type = 'ingest' " +
      "and generic_file_identifier = '' order by date_time desc limit 1"
    if @ctime_query.nil?
      @ctime_query = @db.prepare(query)
    end
    result_set = @ctime_query.execute(object_pid)
    result_set.each_hash do |row|
      timestamp = row['date_time']
    end
    timestamp
  end

  def get_etag(object_identifier)
    etag = nil
    query = "select etag from processed_items where object_identifier = ? " +
      "and action = 'Ingest' and status = 'Success' " +
      "order by updated_at desc limit 1"
    if @etag_query.nil?
      @etag_query = @db.prepare(query)
    end
    result_set = @etag_query.execute(object_identifier)
    result_set.each_hash do |row|
      etag = row['etag']
    end
    etag
  end

  # Get the IntellectualObject's DPN UUID from the DPN ingest event.
  # This will be nil for 99.9% of all objects on the demo server.
  def get_dpn_uuid(object_pid)
    dpn_url = nil
    dpn_uuid = nil
    query = "select outcome_detail from premis_events_solr " +
      "where intellectual_object_id = ? and outcome_information like 'DPN%' " +
      "order by date_time desc limit 1"
    if @uuid_query.nil?
      @uuid_query = @db.prepare(query)
    end
    result_set = @uuid_query.execute(object_pid)
    result_set.each_hash do |row|
      dpn_url = row['outcome_information']
    end
    if !dpn_url.nil?
      tar_file = dpn_url.split('/').last
      if tar_file.end_with?('.tar')
        dpn_uuid = tar_file.sub(/\.tar$/, '')
      end
    end
    dpn_uuid
  end

  # Import all GenericFiles through the REST API.
  # Param obj_pid is the Solr pid of the IntellectualObject
  # whose files we want to import. Param obj_identifier is
  # the intellectual object identifier. E.g. "test.edu/photo_collection"
  # Param new_obj_id is the numeric primary key of the
  # intellectual object in pharos.
  def import_files(obj_pid, obj_identifier, new_obj_id)
    query = "SELECT id, file_format, uri, size, intellectual_object_id, " +
      "identifier, created_at, updated_at FROM generic_files " +
      "WHERE intellectual_object_id = ?"
    if @file_query.nil?
      @file_query = @db.prepare(query)
    end
    result_set = @file_query.execute(obj_pid)
    while true
      files = get_file_batch(result_set, obj_identifier)
      break if files.count == 0
      save_file_batch(files, new_obj_id)
    end
  end

  # Saves a batch of files, with events and checksums,
  # through the REST API. Param files is a list of generic
  # files, and new_object_id is the new primary key
  # identifier of the IntellectualObject in Pharos (integer).
  def save_file_batch(files, new_obj_id)
    url = "#{@base_url}/api/v2/files/#{new_obj_id}/create_batch"
    resp = api_post_json(url, files.to_json)
    if resp.code != '201'
      @log.write("Error saving #{files.count} files #{files[0]['identifier']}...\n\n")
      @log.write(resp.body)
      exit(1)
    end
  end

  def get_file_batch(result_set, obj_identifier)
    count = 0
    files = []
    result_set.each_hash do |row|
      files.push(get_file(row, obj_identifier))
      count += 1
      break if count == @batch_size
    end
    files
  end

  # Returns one file, along with its checksums and events.
  # Param row is a row of GenericFile data from the SQL db.
  # Param obj_identifier is the identifier of this
  # file's parent object. E.g. "test.edu/photo_collection"
  def get_file(row, obj_identifier)
    gf_pid = row['id']
    gf_identifier = row['identifier']
    obj_id = @new_id_for[row['intellectual_object_id']]
    gf = {}
    gf['file_format'] = row['file_format']
    gf['uri'] = row['uri']
    gf['size'] = row['size']
    gf['intellectual_object_id'] = obj_id
    gf['identifier'] = gf_identifier
    gf['state'] = row['state']
    gf['created_at'] = row['created_at']
    gf['updated_at'] = row['updated_at']
    gf['checksums_attributes'] = get_checksums(gf_pid, gf_identifier)
    gf['premis_events_attributes'] = get_file_events(gf_pid, obj_identifier)
    gf
  end

  # Returns the events for the specified generic file.
  # We don't have to batch these, because there are typically
  # only 6-10 events per generic file. Some outliers may have
  # 15 or so, but that's about as high as it goes.
  def get_file_events(gf_pid, obj_identifier)
    query = "select intellectual_object_id, institution_id, " +
      "identifier, event_type, date_time, detail, " +
      "outcome, outcome_detail, outcome_information, " +
      "object, agent, generic_file_id, generic_file_identifier " +
      "from premis_events_solr where generic_file_id = ?"
    if @file_events_query.nil?
      @file_events_query = @db.prepare(query)
    end
    result_set = @file_events_query.execute(gf_pid)
    events = []
    result_set.each_hash do |row|
      events.push(get_event(row, obj_identifier))
    end
    events
  end

  # Returns the checksums for the specified generic file.
  # Param gf_pid is the pid of the GenericFile whose checksums
  # we want to save.
  def get_checksums(gf_pid, gf_identifier)
    query = "SELECT algorithm, datetime, digest, generic_file_id " +
      "FROM checksums where generic_file_id = ?"
    if @checksum_query.nil?
      @checksum_query = @db.prepare(query)
    end
    checksums = []
    result_set = @checksum_query.execute(gf_pid)
    result_set.each_hash do |row|
      cs = {}
      cs['algorithm'] = row['algorithm']
      cs['datetime'] = row['datetime']
      cs['digest'] = row['digest']
      checksums.push(cs)
    end
    checksums
  end

  # Import object-level Premis events through the REST API.
  # Param obj_pid is the pid of the intellectual object
  # to which this event belongs (even if it's a file-level
  # event). Param obj_identifier is the intellectual
  # object identifier.
  def import_object_level_events(obj_pid, obj_identifier)
    query = "select intellectual_object_id, institution_id, " +
      "identifier, event_type, date_time, detail, " +
      "outcome, outcome_detail, outcome_information, " +
      "object, agent, generic_file_id, generic_file_identifier " +
      "from premis_events_solr where generic_file_id is null and " +
      "intellectual_object_id = ?"
    if @obj_events_query.nil?
      @obj_events_query = @db.prepare(query)
    end
    result_set = @obj_events_query.execute(obj_pid)
    result_set.each_hash do |row|
      id = import_event(row, obj_identifier)
      puts "    Saved event #{row['event_type']} for #{row['identifier']} with id #{id}"
    end
  end

  # Import a single Premis event through the REST API.
  def import_event(row, obj_identifier)
    event = get_event(row, obj_identifier)
    url = "#{@base_url}/api/v2/events"
    resp = api_post_json(url, event.to_json)
    if resp.code != '201'
      @log.write("Error saving event #{event['identifier']}\n\n")
      @log.write(resp.body)
      exit(1)
    end
    data = JSON.parse(resp.body)
    return data['id']
  end

  # Returns a single event, which may or may not have a
  # generic_file_identifier.
  def get_event(row, obj_identifier)
    obj_id = @new_id_for[row['intellectual_object_id']]
    gf_id = @new_id_for[row['generic_file_id']]
    inst_id = @new_id_for[row['institution_id']]
    event = {}
    event['intellectual_object_id'] = obj_id
    event['generic_file_id'] = gf_id
    event['institution_id'] = inst_id
    event['identifier'] = row['identifier']
    event['event_type'] = row['event_type']
    event['date_time'] = row['date_time']
    event['detail'] = row['detail']
    event['outcome'] = ucfirst(row['outcome'])
    event['outcome_detail'] = row['outcome_detail']
    event['outcome_information'] = row['outcome_information']
    event['object'] = row['object']
    event['agent'] = row['agent']
    event['generic_file_identifier'] = row['generic_file_identifier']
    event['intellectual_object_identifier'] = obj_identifier
    event
  end

  # Import all WorkItems through the REST API.
  def import_work_items(how_many)
    query = "SELECT id, created_at, updated_at, name, etag, bucket, " +
      "user, institution, note, action, stage, status, outcome, " +
      "bag_date, date, retry, reviewed, object_identifier, " +
      "generic_file_identifier, state, node, pid, needs_admin_review " +
      "FROM processed_items"
    query += " limit #{how_many}" if how_many
    if @work_items_query.nil?
      @work_items_query = @db.prepare(query)
    end
    result_set = @work_items_query.execute
    result_set.each_hash do |row|
      id = import_work_item(row)
      puts "Saved ProcessedItem #{row['id']} as WorkItem #{id}"
      if !row['state'].nil? && row['state'] != ''
        state_id = import_work_item_state(row, id)
        puts "  Saved state for ProcessedItem #{row['id']} as WorkItemState #{state_id}"
      end
    end
  end

  # Import a single WorkItem through the REST API.
  def import_work_item(row)
    inst_name = nil
    if !row['object_identifier'].nil? && row['object_identifier'] != ''
      inst_name = row['object_identifier'].split('/')[0]
    elsif !row['bucket'].nil? && row['bucket'] != ''
      inst_name = row['bucket'].sub('aptrust.receiving.test.', '')
    end
    item = {}
    item['created_at'] = row['created_at']
    item['updated_at'] = row['updated_at']
    item['intellectual_object_id'] = @id_for_name[row['object_identifier']]
    item['generic_file_id'] = @id_for_name[row['generic_file_identifier']]
    item['name'] = row['name']
    item['etag'] = row['etag']
    item['bucket'] = row['bucket']
    item['user'] = row['user']
    item['note'] = row['note']
    item['action'] = row['action']
    item['stage'] = row['stage']
    item['status'] = ucfirst(row['status'])
    item['outcome'] = row['outcome']
    item['bag_date'] = row['bag_date']
    item['date'] = row['date']
    item['retry'] = row['retry']
    item['object_identifier'] = row['object_identifier']
    item['generic_file_identifier'] = row['generic_file_identifier']
    item['node'] = row['node']
    item['pid'] = row['pid']
    item['needs_admin_review'] = row['needs_admin_review']
    item['institution_id'] = @id_for_name[inst_name]
    item['queued_at'] = nil
    item['size'] = nil
    item['stage_started_at'] = nil

    url = "#{@base_url}/api/v2/items"
    resp = api_post_json(url, item.to_json)
    if resp.code != '201'
      @log.write("Error saving WorkItem #{item['id']}\n\n")
      @log.write(resp.body)
      exit(1)
    end
    data = JSON.parse(resp.body)
    return data['id']
  end

  # Import WorkItemState for one WorkItem through the REST API.
  def import_work_item_state(row, work_item_id)
    state = {}
    state['work_item_id'] = work_item_id
    state['action'] = row['action']
    state['state'] = row['state']

    url = "#{@base_url}/api/v2/item_state"
    resp = api_post_json(url, state.to_json)
    if resp.code != '201'
      @log.write("Error saving WorkItem #{row['id']}\n\n")
      @log.write(resp.body)
      exit(1)
    end
    data = JSON.parse(resp.body)
    return data['id']
  end

  # Converts the first letter of string to upper case.
  def ucfirst(string)
    if !string.nil? && string != ''
      string[0] = string[0,1].upcase
    end
    string
  end

  def load_institutions
    # Get Solr institution pids from the SQL db.
    pid_for = {}
    query = "select id, identifier from institutions"
    if @inst_query.nil?
      @inst_query = @db.prepare(query)
    end
    result_set = @inst_query.execute
    result_set.each_hash do |row|
      # Sample entry: pids['miami.edu'] = 'aptrust-test:350660'
      pid_for[row['identifier']] = row['id']
      @name_of[row['id']] = row['identifier']
    end

    # Load institutions from Pharos and map old Solr pid to new id
    url = @base_url + '/api/v2/institutions'
    resp = api_get(url, nil)
    if resp.code != '200'
      puts "Error getting institutions from Pharos"
      puts resp.body
      exit(1)
    end
    data = JSON.parse(resp.body)
    data['results'].each do |inst|
      solr_pid = pid_for[inst['identifier']]
      @new_id_for[solr_pid] = inst['id']
      @id_for_name[inst['identifier']] = inst['id']
      puts "#{inst['identifier']} has id #{inst['id']}"
    end
  end

  def api_get(url, params)
    is_https = url.start_with?('https')
    uri = URI(url)
    uri.query = URI.encode_www_form(params) unless params.nil?
    Net::HTTP.start(uri.host, uri.port, use_ssl: is_https,
                    verify_mode: OpenSSL::SSL::VERIFY_NONE) do |http|
      request = Net::HTTP::Get.new(uri)
      set_headers(request)
      http.request(request)
    end
  end

  def api_post(url, hash)
    is_https = url.start_with?('https')
    uri = URI(url)
    Net::HTTP.start(uri.host, uri.port, use_ssl: is_https,
                    verify_mode: OpenSSL::SSL::VERIFY_NONE) do |http|
      request = Net::HTTP::Post.new(uri)
      set_headers(request)
      request.set_form_data(hash)
      http.request(request)
    end
  end

  def api_post_json(url, json_string)
    is_https = url.start_with?('https')
    uri = URI(url)
    Net::HTTP.start(uri.host, uri.port, use_ssl: is_https,
                    verify_mode: OpenSSL::SSL::VERIFY_NONE) do |http|
      request = Net::HTTP::Post.new(uri)
      set_headers(request)
      request.body = json_string
      http.request(request)
    end
  end

  def api_put(url, hash)
    is_https = url.start_with?('https')
    uri = URI(url)
    Net::HTTP.start(uri.host, uri.port, use_ssl: is_https,
                    verify_mode: OpenSSL::SSL::VERIFY_NONE) do |http|
      set_headers(request)
      request = Net::HTTP::Put.new(uri, hash)
      http.request(request)
    end
  end

  def set_headers(request)
    request['Content-Type'] = 'application/json; charset=utf-8'
    request['Accept'] = 'application/json'
    request['X-Pharos-API-User'] = 'system@aptrust.org'
    request['X-Pharos-API-Key'] = @api_key
  end

end

if __FILE__ == $0
  api_key = ARGV[0]
  if api_key.nil?
    puts "Usage: api_importer.rb <api_key>"
    puts "API key is the admin API key for the demo server"
    exit(1)
  end
  importer = APIImporter.new(api_key)
  importer.run(nil)
end
