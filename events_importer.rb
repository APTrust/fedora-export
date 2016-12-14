require 'sqlite3'

# Importer imports data dumped from Solr into a local SQLite database.
# The SQLite database comes from the Fluctus rake task
# `rake fluctus:dump_repository`
#
# Bugs in Hydra/Fedora prevent the rake task from access all PREMIS
# events, so we have to dump those out directly from Solr, and then
# import them into the SQLite DB with this script. This script assumes
# fedora_export.db is in the same dir as the script.
class Importer

  def initialize
    @db = SQLite3::Database.new("solr_dump/fedora_export.db")
  end

  def drop_table
    @db.execute("drop table if exists premis_events_solr")
  end

  def make_tables
    schema = <<schema
CREATE TABLE IF NOT EXISTS premis_events_solr (
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
         generic_file_uri TEXT
      )
schema
    @db.execute(schema)
  end

  # Events come from
  # wget "http://test.aptrust.org:8080/solr/demo/select?q=event_type_ssim%3A*&start=0&rows=2000000000&wt=ruby&indent=true" -O solr_dump/events.rb
  # You can get other objects with these commands.
  # wget "http://test.aptrust.org:8080/solr/demo/select?q=has_model_ssim%3A%22info%3Afedora%2Fafmodel%3AGenericFile%22&start=0&rows=200000000&wt=ruby&indent=true" -O solr_dump/files.rb
  # wget "http://test.aptrust.org:8080/solr/demo/select?q=has_model_ssim%3A%22info%3Afedora%2Fafmodel%3AInstitution%22&start=0&rows=200000000&wt=ruby&indent=true" -O solr_dump/institutions.rb
  # wget "http://test.aptrust.org:8080/solr/demo/select?q=has_model_ssim%3A%22info%3Afedora%2Fafmodel%3AIntellectualObject%22&start=0&rows=200000000&wt=ruby&indent=true" -O solr_dump/objects.rb
  def copy_events
    # This is a ruby file, but it's too big to read into memory,
    # so we'll parse it one record at a time.
    found_response = false
    record = ''
    count = 0
    File.open('solr_dump/events.rb').each do |line|
      if !found_response
        if line.strip.start_with?("'response'=>")
          found_response = true
        end
        next
      end
      stripped = line.strip
      record += stripped
      if stripped.end_with?('},') || stripped.end_with?('}]')
        copy_event(record.chop) # remove trailing comma
        record = ''
        count += 1
        if count % 1000 == 0
          puts count
        end
      end
    end
  end

  # Copy an event record into DB table premis_events_solr
  def copy_event(record)
    data = nil
    begin
      data = eval(record)
    rescue Exception => ex
      if record.include?("facet_") || record == '}'
        # Reached end of data
        return
      else
        puts "Invalid record: #{record}"
      end
    end
    statement = "insert into premis_events_solr (
         intellectual_object_id,
         generic_file_id,
         institution_id,
         generic_file_identifier,
         identifier,
         event_type,
         date_time,
         detail,
         outcome,
         outcome_detail,
         outcome_information,
         object,
         agent,
         timestamp,
         generic_file_uri) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    @db.execute(statement,
                get_value(data, 'intellectual_object_id_ssim'),
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

  def get_value(data, key)
    if data.has_key?(key) == false ||  data[key].nil? || data[key] == []
      return ''
    end
    return data[key][0]
  end

end

if __FILE__ == $0
  i = Importer.new
  i.drop_table
  i.make_tables
  i.copy_events
end
