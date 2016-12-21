# encoding: utf-8
#
# DO NOT DELETE THE ENCODING LINE ABOVE!!!
# Without it, Ruby assumes ASCII, while our REST API and the SQLite DB
# both assume UTF-8, and everything breaks.

require 'fileutils'
require 'json'
require 'securerandom'
require 'sqlite3'

# SqlToSql imports data from the SQLite database (which is a dump
# of our old Fedora data) into the Pharos database.
class SqlToSql

  def initialize
    @source_db = SQLite3::Database.new("solr_dump/export_20161219.1.db")
    @source_db.results_as_hash = true
    @source_db.execute('PRAGMA encoding = "UTF-8"')

    # Copy the empty Pharos db (with all current migrations) to a new
    # file, so we can start filling it.
    FileUtils.copy("solr_dump/empty_pharos.db", "solr_dump/pharos.db")
    @dest_db = SQLite3::Database.new("solr_dump/pharos.db")
    @dest_db.execute('PRAGMA encoding = "UTF-8"')

    @new_id_for = {} # Hash: key is old Solr pid, value is new numeric id
    @name_of = {} # Hash: key is Solr pid, value is institution domain name
    @batch_size = 100
    @id_for_name = {}

    # Map APTrust 1.0 event types to correct LOC PREMIS event types.
    # The only event type not in the LOC standard is 'access assignment',
    # but we still have to record that.
    @event_type_map = {
      'access_assignment' => 'access assignment',
      'delete' => 'deletion',
      'fixity_check' => 'fixity check',
      'fixity_generation' => 'message digest calculation',
      'identifier_assignment' => 'identifier assignment',
      'ingest' => 'ingestion'
    }
  end

  # Run the import job. If limit is specified (an integer),
  # this will import only the specified number of objects
  # and work items.
  def run(limit, offset)
    @log = File.open('import.log', 'w')
    create_indexes
    import_institutions
    import_users
    import_objects(limit, offset)
    import_work_items(limit, offset)
    @log.close
  end

  def create_indexes
    puts "Creating SQLite indexes"
    @source_db.execute("create index if not exists ix_gf_obj_id " +
                       "on generic_files(intellectual_object_id)")
    @source_db.execute("create index if not exists ix_cs_gf_id " +
                       "on checksums(generic_file_id)")
    @source_db.execute("create index if not exists ix_items_obj_identifier " +
                       "on processed_items(object_identifier)")
    @source_db.execute("create index if not exists ix_event_obj_id " +
                       "on premis_events_solr(intellectual_object_id)")
    @source_db.execute("create index if not exists ix_event_gf_id " +
                       "on premis_events_solr(generic_file_id)")
  end

  def import_institutions
    if @insert_inst.nil?
      stmt = "insert into institutions (name, brief_name, identifier, " +
        "dpn_uuid, state, created_at, updated_at) values (?,?,?,?,?,?,?)"
      @insert_inst = @dest_db.prepare(stmt)
    end
    query = "select id, name, brief_name, " +
      "identifier, dpn_uuid from institutions"
    @source_db.execute(query).each do |row|
      solr_pid = row['id']
      @insert_inst.execute(row['name'],
                           row['brief_name'],
                           row['identifier'],
                           row['dpn_uuid'],
                           'A',
                           '2015-01-01T00:00:00Z',
                           '2015-01-01T00:00:00Z')
      id = last_insert_id
      #pid_for[row['identifier']] = solr_pid
      @name_of[solr_pid] = row['identifier']
      @new_id_for[solr_pid] = id
      @id_for_name[row['identifier']] = id
    end
  end

  def import_users
    if @insert_users.nil?
      stmt = "insert into users (name, email, phone_number, created_at, " +
        "updated_at, encrypted_password, reset_password_token, " +
        "remember_created_at, sign_in_count, current_sign_in_at, " +
        "last_sign_in_at, current_sign_in_ip, last_sign_in_ip, " +
        "institution_id, encrypted_api_secret_key) " +
        "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      @insert_users = @dest_db.prepare(stmt)
    end
    query = "select name, email, phone_number, created_at, " +
      "updated_at, encrypted_password, reset_password_token, " +
      "remember_created_at, sign_in_count, current_sign_in_at, " +
      "last_sign_in_at, current_sign_in_ip, last_sign_in_ip, " +
      "institution_pid, encrypted_api_secret_key from users"
    @dest_db.transaction
    @source_db.execute(query).each do |row|
      inst_id = @new_id_for[row['institution_pid']]
      token = row['reset_password_token']
      if token.nil? || token.strip == ''
        token = SecureRandom.hex(64)
      end
      @insert_users.execute(row['name'],
                            row['email'],
                            row['phone_number'],
                            row['created_at'],
                            row['updated_at'],
                            row['encrypted_password'],
                            token,
                            row['remember_created_at'],
                            row['sign_in_count'],
                            row['current_sign_in_at'],
                            row['last_sign_in_at'],
                            row['current_sign_in_ip'],
                            row['last_sign_in_ip'],
                            inst_id,
                            row['encrypted_api_secret_key'])
    end
    @dest_db.commit
  end

  def import_objects(limit, offset)
    if @obj_query.nil?
      query = "SELECT id, identifier, title, description, alt_identifier, " +
        "access, bag_name, institution_id, state FROM intellectual_objects"
      query += " limit #{limit}" unless limit.nil?
      query += " offset #{offset}" unless offset.nil?
      @obj_query = @source_db.prepare(query)
    end
    result_set = @obj_query.execute
    result_set.each_hash do |row|
      pid = row['id'].strip.force_encoding('UTF-8')
      id = import_object(row)
      @new_id_for[pid] = id
      @id_for_name[row['identifier']] = id
      import_object_level_events(pid, row['identifier'])
      import_files(pid, row['identifier'], id)
      puts "Saved object #{row['identifier']} with id #{id}"
    end
  end

  # Send one IntellectualObject record to Pharos through the API.
  # Param row is a row of data selected from the db.
  # Returns the id of the saved IntellectualObject.
  def import_object(row)
    if @obj_insert.nil?
      stmt = "insert into intellectual_objects(title, description, identifier, " +
        "alt_identifier, access, bag_name, institution_id, state, etag, dpn_uuid, " +
        "created_at, updated_at) values (?,?,?,?,?,?,?,?,?,?,?,?)"
      @obj_insert = @dest_db.prepare(stmt)
    end
    created_at = get_obj_create_time(row['id'])
    @obj_insert.execute(row['title'],
                        row['description'],
                        row['identifier'],
                        row['alt_identifier'],
                        row['access'],
                        row['bag_name'],
                        @new_id_for[row['institution_id']],
                        row['state'],
                        get_etag(row['identifier']),
                        get_dpn_uuid(row['id']),
                        created_at,
                        created_at)
    return last_insert_id
  end

  def last_insert_id
    if @last_id_query.nil?
      @last_id_query = @dest_db.prepare("SELECT last_insert_rowid()")
    end
    id = nil
    @last_id_query.execute().each do |row|
      id = row[0]
    end
    return id
  end

  def get_obj_create_time(object_pid)
    timestamp = nil
    if @ctime_query.nil?
      query = "select date_time from premis_events_solr " +
        "where intellectual_object_id = ? and event_type = 'ingest' " +
        "and generic_file_identifier = '' order by date_time desc limit 1"
      @ctime_query = @source_db.prepare(query)
    end
    result_set = @ctime_query.execute(object_pid)
    result_set.each_hash do |row|
      timestamp = row['date_time']
    end
    timestamp
  end

  def get_etag(object_identifier)
    etag = nil
    if @etag_query.nil?
      query = "select etag from processed_items where object_identifier = ? " +
        "and action = 'Ingest' and status = 'Success' " +
        "order by updated_at desc limit 1"
      @etag_query = @source_db.prepare(query)
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
    if @uuid_query.nil?
      query = "select outcome_detail from premis_events_solr " +
        "where intellectual_object_id = ? and outcome_information like 'DPN%' " +
        "order by date_time desc limit 1"
      @uuid_query = @source_db.prepare(query)
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
    if @file_query.nil?
      query = "SELECT id, file_format, uri, size, intellectual_object_id, " +
        "identifier, created_at, updated_at FROM generic_files " +
        "WHERE intellectual_object_id = ?"
      @file_query = @source_db.prepare(query)
    end
    pids_saved = []
    result_set = @file_query.execute(obj_pid)
    @dest_db.transaction
    result_set.each_hash do |row|
      gf_pid = row['id']
      new_gf_id = import_file(row, obj_identifier, new_obj_id)
      @id_for_name[row['identifier']] = new_gf_id
      @new_id_for[gf_pid] = new_gf_id
      pids_saved.push(gf_pid)
    end
    @dest_db.commit

    # Save each object's events and checksums.
    # We don't need to track the new ids of those records.
    pids_saved.each do |gf_pid|
      import_file_events(gf_pid, obj_identifier)
      import_checksums(gf_pid)
    end
  end


  # Saves a GenericFile record to the destination (Pharos) DB.
  # Param row is a row of GenericFile data from the SQL db.
  # Param obj_identifier is the identifier of this
  # file's parent object. E.g. "test.edu/photo_collection"
  def import_file(row, obj_identifier, new_obj_id)
    if @insert_file.nil?
      stmt = "insert into generic_files(file_format, uri, size, identifier, " +
        "intellectual_object_id, permissions, state, created_at, updated_at) " +
        "values (?,?,?,?,?,?,?,?,?)"
      @insert_file = @dest_db.prepare(stmt)
    end
    @insert_file.execute(row['file_format'],
                         row['uri'],
                         row['size'],
                         row['identifier'],
                         new_obj_id,
                         nil,
                         row['state'],
                         row['created_at'],
                         row['updated_at'])
    return last_insert_id
  end

  # Imports the events associated with a specific generic file.
  # We don't have to batch these, because there are typically
  # only 6-10 events per generic file. Some outliers may have
  # 15 or so, but that's about as high as it goes.
  def import_file_events(gf_pid, obj_identifier)
    if @file_events_query.nil?
      query = "select intellectual_object_id, institution_id, " +
        "identifier, event_type, date_time, detail, " +
        "outcome, outcome_detail, outcome_information, " +
        "object, agent, generic_file_id, generic_file_identifier " +
        "from premis_events_solr where generic_file_id = ?"
      @file_events_query = @source_db.prepare(query)
    end
    if @file_events_insert.nil?
      stmt = "insert into premis_events (identifier, event_type, date_time, " +
        "outcome_detail, detail, outcome_information, object, agent, " +
        "intellectual_object_id, generic_file_id, institution_id, outcome, " +
        "intellectual_object_identifier, generic_file_identifier, old_uuid, " +
        "created_at, updated_at) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      @file_events_insert = @dest_db.prepare(stmt)
    end
    result_set = @file_events_query.execute(gf_pid)
    @dest_db.transaction
    result_set.each_hash do |row|
      obj_id = @new_id_for[row['intellectual_object_id']]
      gf_id = @new_id_for[row['generic_file_id']]
      inst_id = @new_id_for[row['institution_id']]
      new_event_type = transform_event_type(row['event_type'], row['outcome_detail'])
      @file_events_insert
        .execute(row['identifier'],
                 new_event_type,
                 row['date_time'],
                 row['outcome_detail'],
                 row['detail'],
                 row['outcome_information'],
                 row['object'],
                 row['agent'],
                 obj_id,
                 gf_id,
                 inst_id,
                 ucfirst(row['outcome']),
                 obj_identifier,
                 row['generic_file_identifier'],
                 nil,
                 row['date_time'],
                 row['date_time'])
    end
    @dest_db.commit
  end

  # Imports the checksums for the specified generic file.
  # Param gf_pid is the pid of the GenericFile whose checksums
  # we want to save.
  def import_checksums(gf_pid)
    if @checksum_query.nil?
      query = "SELECT algorithm, datetime, digest, generic_file_id " +
        "FROM checksums where generic_file_id = ?"
      @checksum_query = @source_db.prepare(query)
    end
    if @checksum_insert.nil?
      stmt = "insert into checksums(algorithm, datetime, digest, " +
        "generic_file_id, created_at, updated_at) values (?,?,?,?,?,?)"
      @checksum_insert = @dest_db.prepare(stmt)
    end
    result_set = @checksum_query.execute(gf_pid)
    @dest_db.transaction
    result_set.each_hash do |row|
      new_gf_id = @new_id_for[gf_pid]
      @checksum_insert.execute(row['algorithm'],
                               row['datetime'],
                               row['digest'],
                               new_gf_id,
                               row['datetime'],
                               row['datetime'])
    end
    @dest_db.commit
  end

  # Import object-level Premis events through the REST API.
  # Param obj_pid is the pid of the intellectual object
  # to which this event belongs (even if it's a file-level
  # event). Param obj_identifier is the intellectual
  # object identifier.
  def import_object_level_events(obj_pid, obj_identifier)
    if @obj_events_query.nil?
      query = "select intellectual_object_id, institution_id, " +
        "identifier, event_type, date_time, detail, " +
        "outcome, outcome_detail, outcome_information, " +
        "object, agent, generic_file_id, generic_file_identifier " +
        "from premis_events_solr where generic_file_id is null and " +
        "intellectual_object_id = ?"
      @obj_events_query = @source_db.prepare(query)
    end
    if @obj_events_insert.nil?
      stmt = "insert into premis_events (identifier, event_type, date_time, " +
        "outcome_detail, detail, outcome_information, object, agent, " +
        "intellectual_object_id, generic_file_id, institution_id, outcome, " +
        "intellectual_object_identifier, generic_file_identifier, old_uuid, " +
        "created_at, updated_at) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      @obj_events_insert = @dest_db.prepare(stmt)
    end
    result_set = @obj_events_query.execute(obj_pid)
    @dest_db.transaction
    result_set.each_hash do |row|
      obj_id = @new_id_for[row['intellectual_object_id']]
      gf_id = nil
      inst_id = @new_id_for[row['institution_id']]
      new_event_type = transform_event_type(row['event_type'], row['outcome_detail'])
      @file_events_insert
        .execute(row['identifier'],
                 new_event_type,
                 row['date_time'],
                 row['outcome_detail'],
                 row['detail'],
                 row['outcome_information'],
                 row['object'],
                 row['agent'],
                 obj_id,
                 gf_id,
                 inst_id,
                 ucfirst(row['outcome']),
                 obj_identifier,
                 row['generic_file_identifier'],
                 nil,
                 row['date_time'],
                 row['date_time'])
    end
    @dest_db.commit
  end


  # Import all WorkItems through the REST API.
  def import_work_items(limit, offset)
    if @work_items_query.nil?
      query = "SELECT id, created_at, updated_at, name, etag, bucket, " +
        "user, institution, note, action, stage, status, outcome, " +
        "bag_date, date, retry, reviewed, object_identifier, " +
        "generic_file_identifier, state, node, pid, needs_admin_review " +
        "FROM processed_items limit ? offset ?"
      @work_items_query = @source_db.prepare(query)
    end
    limit ||= 100
    offset ||= 0
    while true
      count = 0
      result_set = @work_items_query.execute(limit, offset)
      result_set.each_hash do |row|
        with_state = ''
        id = import_work_item(row)
        if !row['state'].nil? && row['state'] != ''
          import_work_item_state(row, id)
          with_state = '(with state)'
        end
        puts "Imported ProcessedItem #{row['id']} as WorkItem #{id} #{with_state}"
        count += 1
      end
      break if count == 0
      offset += count
    end
  end

  # Import a single WorkItem.
  def import_work_item(row)
    if @work_item_insert.nil?
      stmt = "insert into work_items(intellectual_object_id, generic_file_id, " +
        "name, etag, bucket, user, note, action, stage, status, outcome, " +
        "bag_date, date, retry, object_identifier, generic_file_identifier, " +
        "node, pid, needs_admin_review, institution_id, queued_at, size, " +
        "stage_started_at, created_at, updated_at) " +
        "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      @work_item_insert = @dest_db.prepare(stmt)
    end
    @work_item_insert
      .execute(
               @id_for_name[row['object_identifier']],
               @id_for_name[row['generic_file_identifier']],
               row['name'],
               row['etag'],
               row['bucket'],
               row['user'],
               row['note'],
               row['action'],
               row['stage'],
               ucfirst(row['status']),
               ucfirst(row['outcome']),
               row['bag_date'],
               row['date'],
               row['retry'],
               row['object_identifier'],
               row['generic_file_identifier'],
               row['node'],
               row['pid'],
               row['needs_admin_review'],
               @id_for_name[row['institution']],
               row['updated_at'],
               nil,
               nil,
               row['created_at'],
               row['updated_at'])
    return last_insert_id
  end

  # Import WorkItemState for one WorkItem through the REST API.
  def import_work_item_state(row, work_item_id)
    if @state_insert.nil?
      stmt = "insert into work_item_states(work_item_id, action, state, " +
        "created_at, updated_at) values (?,?,?,?,?)"
      @state_insert = @dest_db.prepare(stmt)
    end
    @state_insert.execute(work_item_id,
                          row['action'],
                          row['state'],
                          row['created_at'],
                          row['updated_at'])
  end

  # Converts the first letter of string to upper case.
  def ucfirst(string)
    if !string.nil? && string != ''
      string[0] = string[0,1].upcase
    end
    string
  end

  # Transform non-standard APTrust 1.0 PREMIS event types to
  # LOC standard event types. There's one special case: replication
  # in the old system was recorded as a second ingest.
  def transform_event_type(event_type, outcome_detail)
    if event_type == 'ingest' && outcome_detail.include?('aptrust.preservation.oregon')
      return 'replication'
    end
    return @event_type_map[event_type]
  end

end

if __FILE__ == $0
  importer = SqlToSql.new
  importer.run(-1, 0)
end
