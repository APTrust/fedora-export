require 'json'
require 'net/http'
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
    @new_id_for = {} # Hash: key is old Solr pid, value is new numeric id
    @name_of = {} # Hash: key is Solr pid, value is institution domain name
    @base_url = 'http://localhost:3000'
  end

  def import_objects(limit)
    query = "SELECT id, identifier, title, description, alt_identifier, " +
      "access, bag_name, institution_id, state FROM intellectual_objects"
    query += " limit #{limit}" if limit
    @db.execute(query) do |row|
      pid = row['id']
      id = import_object(row)
      @new_id_for[pid] = id
      import_files(pid, row['identifier'])
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
    obj['intellectual_object[etag]'] = get_etag(row['id'])
    obj['intellectual_object[created_at]'] = get_obj_create_time(row['id'])
    obj['intellectual_object[dpn_uuid]'] = get_dpn_uuid(row['id'])

    inst = @name_of[row['institution_id']]
    url = "#{@base_url}/api/v2/objects/#{inst}.json"
    resp = api_post(url, obj)
    if resp.code != '201'
      puts "Error saving object #{obj['intellectual_object[identifier]']}"
      puts resp.body
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
    @db.execute(query, object_pid) do |row|
      timestamp = row['date_time']
    end
    timestamp
  end

  def get_etag(object_pid)
    etag = nil
    query = "select etag from processed_items where object_identifier = ? " +
      "and action = 'Ingest' and status = 'Success' " +
      "order by updated_at desc limit 1"
    @db.execute(query, object_pid) do |row|
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
    @db.execute(query, object_pid) do |row|
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
  def import_files(obj_pid, obj_identifier)
    query = "SELECT id, file_format, uri, size, intellectual_object_id, " +
      "identifier, created_at, updated_at FROM generic_files " +
      "WHERE intellectual_object_id = ?"
    @db.execute(query, obj_pid) do |row|
      pid = row['id']
      id = import_file(row, obj_identifier)
      @new_id_for[pid] = id
      import_checksums(pid, row['identifier'])
      puts "  Saved file #{row['identifier']} with id #{id}"
    end
  end

  # Import a single GenericFile through the REST API.
  # Param row is a row of GenericFile data from the SQL db.
  # Param obj_identifier is the identifier of this
  # file's parent object. E.g. "test.edu/photo_collection"
  def import_file(row, obj_identifier)
    obj_id = @new_id_for[row['intellectual_object_id']]
    gf = {}
    gf['generic_file[file_format]'] = row['file_format']
    gf['generic_file[uri]'] = row['uri']
    gf['generic_file[size]'] = row['size']
    gf['generic_file[intellectual_object_id]'] = obj_id
    gf['generic_file[identifier]'] = row['identifier']
    gf['generic_file[created_at]'] = row['created_at']
    gf['generic_file[updated_at]'] = row['updated_at']

    escaped_identifier = URI.escape(obj_identifier).gsub('/', '%2F')
    url = "#{@base_url}/files/#{escaped_identifier}"
    resp = api_post(url, gf)
    if resp.code != '201'
      puts "Error saving file #{row['identifier']}"
      puts resp.body
      exit(1)
    end
    data = JSON.parse(resp.body)
    return data['id']
  end

  # Import checksums through the REST API.
  # Param gf_pid is the pid of the GenericFile whose checksums
  # we want to save.
  def import_checksums(gf_pid, gf_identifier)
    query = "SELECT algorithm, datetime, digest, generic_file_id " +
      "FROM checksums where generic_file_id = ?"
    @db.execute(query, gf_pid) do |row|
      id = import_checksum(row, gf_identifier)
      puts "    Saved checksum #{row['algorithm']} #{row['digest']}"
    end
  end

  # Import a single checksum through the REST API.
  def import_checksum(row, gf_identifier)
    cs = {}
    cs['checksum[algorithm]'] = row['algorithm']
    cs['checksum[datetime]'] = row['datetime']
    cs['checksum[digest]'] = row['digest']

    escaped_identifier = URI.escape(gf_identifier).gsub('/', '%2F')
    url = "#{@base_url}/api/v2/checksums/#{escaped_identifier}"
    resp = api_post(url, cs)
    if resp.code != '201'
      puts "Error saving checksum #{row['algorithm']} for #{gf_identifier}"
      puts resp.body
      exit(1)
    end
    data = JSON.parse(resp.body)
    return data['id']
  end

  # Import all Premis events through the REST API.
  # Some of these need transformations.
  def import_events

  end

  # Import a single Premis event through the REST API.
  def import_event

  end

  # Import all WorkItems through the REST API.
  def import_work_items

  end

  # Import a single WorkItem through the REST API.
  def import_work_item

  end

  # Import WorkItemState for one WorkItem through the REST API.
  def import_work_item_state

  end

  def load_institutions
    # Get Solr institution pids from the SQL db.
    pid_for = {}
    query = "select id, identifier from institutions"
    @db.execute(query) do |row|
      # pids['miami.edu'] = 'aptrust-test:350660'
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
      puts "#{inst['identifier']} has id #{inst['id']}"
    end
  end

  def api_get(url, params)
    uri = URI(url)
    uri.query = URI.encode_www_form(params) unless params.nil?
    Net::HTTP.start(uri.host, uri.port) do |http|
      request = Net::HTTP::Get.new(uri)
      set_headers(request)
      http.request(request)
    end
  end

  def api_post(url, hash)
    uri = URI(url)
    Net::HTTP.start(uri.host, uri.port) do |http|
      request = Net::HTTP::Post.new(uri)
      set_headers(request)
      request.set_form_data(hash)
      http.request(request)
    end
  end

  def api_put(url, hash)
    uri = URI(url)
    Net::HTTP.start(uri.host, uri.port) do |http|
      set_headers(request)
      request = Net::HTTP::Put.new(uri, hash)
      http.request(request)
    end
  end

  def set_headers(request)
    request['Content-Type'] = 'application/json'
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
  importer.load_institutions
  importer.import_objects(10)
end
