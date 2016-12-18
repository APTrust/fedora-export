# encoding: utf-8
#
# DO NOT DELETE THE ENCODING LINE ABOVE!!!
# Without it, Ruby assumes ASCII, while our REST API and the SQLite DB
# both assume UTF-8, and everything breaks.

require 'sqlite3'

# SolrFileImporter imports data dumped from Solr into a local SQLite database.
# The Solr dump comes from a simple wget call that dumps Solr records
# into a local .rb file. Dumping and then importing the massive .rb files
# is actually much faster than making direct API calls to Solr, getting records
# by the batch, and then inserting them into SQL.
#
# Bugs in Hydra/Fedora prevent other, more obvious tasks (such as rake tasks
# and direct API calls) from accessing all PREMIS events. The Fedora/Hydra/Solr
# stack is also too unstable to run large-scale, long-running exports.
# So we dump data directly from Solr to a set of .rb files, and then
# import them into the SQLite DB with this script. This script assumes
# the rake task `export:export_checksums` has already created the SQLite db.
#
# Records come from these queries.
# We only get objects, files, and events from Solr.
# All else comes from `rbenv exec export:export_checksums[fedora_export.db]`
#
# wget "https://repository.aptrust.org:8080/solr/demo/select?q=event_type_ssim%3A*&start=0&rows=2000000000&wt=ruby&indent=true" -O solr_dump/events.rb
# wget "https://repository.aptrust.org:8080/solr/demo/select?q=has_model_ssim%3A%22info%3Afedora%2Fafmodel%3AGenericFile%22&start=0&rows=200000000&wt=ruby&indent=true" -O solr_dump/files.rb
# wget "https://repository.aptrust.org:8080/solr/demo/select?q=has_model_ssim%3A%22info%3Afedora%2Fafmodel%3AIntellectualObject%22&start=0&rows=200000000&wt=ruby&indent=true" -O solr_dump/objects.rb

class SolrFileImporter

  def initialize(db_file, data_dir)
    @db = SQLite3::Database.new(db_file)
    @data_dir = data_dir
    @batch_size = 1000
    @obj_start = 0
    @file_start = 0
    @event_start = 0
    create_tables()
    @obj_statement = @db.prepare('INSERT INTO intellectual_objects (id, identifier, title, ' +
                 'description, alt_identifier, access, bag_name, institution_id, ' +
                 'state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)')
    @file_statement = @db.prepare('INSERT INTO generic_files (id, file_format, uri, size, ' +
                 'intellectual_object_id, identifier, state, created_at, ' +
                 'updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)')
    @event_statement = @db.prepare('insert into premis_events_solr (intellectual_object_id, ' +
                'generic_file_id, institution_id, generic_file_identifier, ' +
                'identifier, event_type, date_time, detail, outcome, ' +
                'outcome_detail, outcome_information, object, agent, ' +
                'timestamp, generic_file_uri) ' +
                'values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)')
  end


  def copy_records(obj_type)
    if !['objects', 'files', 'events'].include?(obj_type)
      puts "Param obj_type must be 'objects', 'files', or 'events'"
      exit(1)
    end
    # This is a ruby file, but it's too big to read into memory,
    # so we'll parse it one record at a time.
    found_response = false
    count = 0
    line_num = 0
    record = ''
    list = []
    File.open("#{@data_dir}/#{obj_type}.rb").each do |line|
      line_num += 1
      if !found_response
        if line.strip.start_with?("'response'=>")
          found_response = true
        end
        next
      end
      stripped = line.strip
      record += stripped
      reached_end_of_records = (stripped == '},')
      if (stripped.end_with?('},') || stripped.end_with?('}]')) && !reached_end_of_records
        begin
          list.push(eval(record.chop))
        rescue Exception => ex
          puts "Parse error, line #{line_num}"
          puts record.chop
          exit(1)
        end
        record = ''
      end
      if list.count == @batch_size || reached_end_of_records
        if obj_type == 'objects'
          insert_objects(list)
        elsif obj_type == 'files'
          insert_files(list)
        else
          insert_events(list)
        end
        count += list.count
        puts "#{count} #{obj_type}"
        list = []
      end
      break if reached_end_of_records
    end
  end

  def insert_objects(obj_list)
    @db.transaction
    obj_list.each do |data|
      institution_id = get_value(data, 'is_part_of_ssim').split('/')[1]
      raise "No institution for #{get_value(data, 'is_part_of_ssim')}" unless institution_id
      access = get_access(data)
      @obj_statement.execute(get_value(data, 'id'),
                             get_value(data, 'desc_metadata__identifier_ssim'),
                             get_value(data, 'desc_metadata__title_tesim'),
                             get_value(data, 'desc_metadata__description_tesim'),
                             get_value(data, 'desc_metadata__alt_identifier_ssim'),
                             access,
                             get_value(data, 'desc_metadata__bag_name_ssim'),
                             institution_id,
                             get_value(data, 'object_state_ssi'))
    end
    @db.commit
  end

  def insert_files(file_list)
    @db.transaction
    file_list.each do |data|
      obj_id = get_value(data, 'is_part_of_ssim').split('/')[1]
      raise "No parent object for #{get_value(data, 'is_part_of_ssim')}" unless obj_id
      @file_statement.execute(get_value(data, 'id'),
                              get_value(data, 'tech_metadata__file_format_ssi'),
                              get_value(data, 'tech_metadata__uri_ssim'),
                              get_value(data, 'tech_metadata__size_lsi'),
                              obj_id,
                              get_value(data, 'tech_metadata__identifier_ssim'),
                              get_value(data, 'object_state_ssi'),
                              get_value(data, 'system_create_dtsi'),
                              get_value(data, 'system_modified_dtsi'))
    end
    @db.commit
  end

  def insert_events(event_list)
    @db.transaction
    event_list.each do |data|
      @event_statement.execute(get_value(data, 'intellectual_object_id_ssim'),
                               get_value(data, 'generic_file_id_ssim'),
                               get_value(data, 'institution_id_ssim'),
                               get_value(data, 'generic_file_identifier_ssim'),
                               get_value(data, 'event_identifier_ssim'),
                               get_value(data, 'event_type_ssim'),
                               get_value(data, 'event_date_time_ssim'),
                               get_value(data, 'event_detail_ssim'),
                               get_value(data, 'event_outcome_ssim'),
                               get_value(data, 'event_outcome_detail_ssim'),
                               get_value(data, 'event_outcome_information_ssim'),
                               get_value(data, 'event_object_ssim'),
                               get_value(data, 'event_agent_ssim'),
                               get_value(data, 'timestamp'),
                               get_value(data, 'generic_file_uri_ssim'))
    end
    @db.commit
  end

  def create_tables
    @db.execute("CREATE TABLE IF NOT EXISTS intellectual_objects (
         id TEXT PRIMARY KEY,
         identifier TEXT,
         title TEXT,
         description TEXT,
         alt_identifier TEXT,
         access TEXT,
         bag_name TEXT,
         institution_id TEXT,
         state TEXT)")
    @db.execute("CREATE TABLE IF NOT EXISTS generic_files (
         id TEXT PRIMARY KEY,
         file_format TEXT,
         uri TEXT,
         size REAL,
         intellectual_object_id TEXT,
         identifier TEXT,
         state TEXT,
         created_at TEXT,
         updated_at TEXT)")
    @db.execute("CREATE TABLE IF NOT EXISTS premis_events_solr (
         intellectual_object_id TEXT,
         generic_file_id TEXT,
         institution_id TEXT,
         generic_file_identifier TEXT,
         identifier TEXT,
         event_type TEXT,
         date_time TEXT,
         detail TEXT,
         outcome TEXT,
         outcome_detail TEXT,
         outcome_information TEXT,
         object TEXT,
         agent TEXT,
         timestamp TEXT,
         generic_file_uri TEXT)")
  end


  def get_access(object_data)
    # Restricted  = discover: 1, read: 0, edit: 1
    # Institution = discover: 0, read: 1, edit: 1
    # Consortia   = discover: 0, read: 2, edit: 1
    read = object_data['read_access_group_ssim']
    return 'restricted' if read.nil? || read.length == 0
    return 'institution' if read.length == 1
    return 'consortia' if read.length == 2
  end

  # Returns the value of data[key] as a scalar value.
  # These values are usually arrays containing a single item,
  # but are occasionally strings or numbers.
  def get_value(data, key)
    if data.has_key?(key) == false ||  data[key].nil? || data[key] == []
      return ''
    end
    value = data[key]
    if value.is_a?(Array)
      return value[0]
    end
    return value
  end
end


if __FILE__ == $0
  path_to_db = ARGV[0]
  data_dir = ARGV[1]
  if path_to_db.nil? || data_dir.nil?
    puts "Usage: solr_file_importer.rb <path_to_db> <data_dir>"
    puts "path_to_db is the path the SQLite DB file"
    puts "data_dir is the directory that contains the Solr dump .rb files"
    exit(1)
  end
  i = SolrFileImporter.new(path_to_db, data_dir)
  i.copy_records('objects')
  i.copy_records('files')
  i.copy_records('events')
end
