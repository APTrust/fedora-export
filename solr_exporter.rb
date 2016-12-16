require 'net/http'
require 'openssl'
require 'sqlite3'

# Export IntellectualObjects, GenericFiles, and PREMIS events
# directly from Solr to SQLite

class SolrExporter

  def initialize(path_to_db)
    @db = SQLite3::Database.new(path_to_db)
    # @base_url = "http://54.172.115.2:8080/solr/production/select"
    @base_url = "http://localhost:8080/solr/production/select"
    @batch_size = 1000
    @obj_start = 0
    @file_start = 0
    @event_start = 0
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

  def export_objects
    while true
      resp = solr_get(next_obj_url(@obj_start))
      if resp.code.to_s != '200'
        puts "Response code #{resp.code} from Solr"
        puts resp.body
        break
      end
      obj_list = parse_response(resp.body)
      if obj_list.length == 0
        break
      end
      insert_objects(obj_list)
      @obj_start += @batch_size
      puts "Objects: #{@obj_start}"
    end
  end

  def export_files
    while true
      resp = solr_get(next_file_url(@file_start))
      if resp.code.to_s != '200'
        puts "Response code #{resp.code} from Solr"
        puts resp.body
        break
      end
      file_list = parse_response(resp.body)
      if file_list.length == 0
        break
      end
      insert_files(file_list)
      @file_start += @batch_size
      puts "Files: #{@file_start}"
    end
  end

  def export_events
    while true
      resp = solr_get(next_event_url(@event_start))
      if resp.code.to_s != '200'
        puts "Response code #{resp.code} from Solr"
        puts resp.body
        break
      end
      event_list = parse_response(resp.body)
      if event_list.length == 0
        break
      end
      insert_events(event_list)
      @event_start += @batch_size
      puts "Events: #{@event_start}"
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

  def parse_response(solr_response)
    data = eval(solr_response)
    data['response']['docs']
  end

  def next_obj_url(start)
    "#{@base_url}?q=active_fedora_model_ssi%3A%22IntellectualObject%22&wt=ruby&indent=true&start=#{start}&rows=#{@batch_size}&sort=system_create_dtsi+asc"
  end

  def next_file_url(start)
    "#{@base_url}?q=active_fedora_model_ssi%3A%22GenericFile%22&wt=ruby&indent=true&start=#{start}&rows=#{@batch_size}&sort=system_create_dtsi+asc"
  end

  def next_event_url(start)
    "#{@base_url}?q=event_type_ssim%3A*&wt=ruby&indent=true&start=#{start}&rows=#{@batch_size}&sort=system_create_dtsi+asc"
  end

  def solr_get(url)
    is_https = url.start_with?('https')
    uri = URI(url)
    Net::HTTP.start(uri.host, uri.port, use_ssl: is_https,
                    verify_mode: OpenSSL::SSL::VERIFY_NONE) do |http|
      request = Net::HTTP::Get.new(uri)
      http.request(request)
    end
  end

  def get_access(object_data)
    # Restricted  = discover: 1, read: 0, edit: 1
    # Institution = discover: 0, read: 1, edit: 1
    # Consortia   = discover: 0, read: 2, edit: 1
    discover = get_value(object_data, 'discover_access_group_ssim')
    return 'restricted' if discover
    read = object_data['read_access_group_ssim']
    if read.nil? || read.length < 1 || read.length > 2
      puts "Can't figure out access for #{obj_data['desc_metadata__identifier_ssim']}"
      puts read
      exit(1)
    end
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
  if path_to_db.nil?
    puts "Usage: solr_export.rb <path_to_db>"
    puts "path_to_db is the path the SQLite DB file"
    exit(1)
  end
  exporter = SolrExporter.new(path_to_db)
  exporter.export_objects
  exporter.export_files
  exporter.export_events
end
