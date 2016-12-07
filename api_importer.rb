require 'sqlite3'
require 'net/http'

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
  end

  def load_institutions
    # Load institutions from Pharos and map name to new id
  end

  def import_objects
    query = "SELECT id, identifier, title, description, alt_identifier, " +
      "access, bag_name, institution_id, state FROM intellectual_objects limit 10"
    @db.execute(query) do |row|
      obj = {}
      obj['identifier'] = row['identifier']
      obj['title'] = row['title']
      obj['description'] = row['description']
      obj['alt_identifier'] = row['alt_identifier']
      obj['access'] = row['identifier']
      obj['bag_name'] = row['identifier']
      obj['state'] = row['state']

      obj['institution_id'] = @new_id_for[row['institution_id']]
      obj['etag'] = get_etag(row['id'])
      obj['created_at'] = get_obj_create_time(row['id'])
      obj['dpn_uuid'] = get_dpn_uuid(row['id'])

      puts obj
    end
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

  # Send one IntellectualObject record to Pharos through the API.
  def import_object

  end

  # Import all GenericFiles through the REST API.
  def import_files

  end

  # Import a single GenericFile through the REST API.
  def import_file

  end

  # Import all Premis events through the REST API.
  # Some of these need transformations.
  def import_events

  end

  # Import a single Premis event through the REST API.
  def import_event

  end

  # Import all checksums through the REST API.
  def import_checksums

  end

  # Import a single checksum through the REST API.
  def import_checksum

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

end

if __FILE__ == $0
  api_key = ARGV[0]
  if api_key.nil?
    puts "Usage: api_importer.rb <api_key>"
    puts "API key is the admin API key for the demo server"
    exit(1)
  end
  importer = APIImporter.new(api_key)
  importer.import_objects
end
