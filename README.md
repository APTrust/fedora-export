# fedora-export

This repo contains scripts to help migrate Fedora data from Fluctus to Pharos.
The basic migration process is:

1. Dump all Fedora data to a SQLite DB using the Fluctus rake task
   `rake fluctus:dump_repository`
2. Because Hydra does not return all PREMIS events when running in
   Rails console (WTF??), we have to dump out PREMIS events directly from
   Solr using:
   `wget "http://test.aptrust.org:8080/solr/demo/select?q=event_type_ssim%3A*&start=0&rows=2000000000&wt=ruby&indent=true" -O solr_dump/events.rb`
   Note that this URL is closed the public. Your IP must be whitelisted for access.
3. Run importer.rb to import the Solr PREMIS events into the SQLite database.
4. For the test/demo server, we'll import Users, Institutions, and Roles using
   the Pharos rake task `rake pharos:transition_Fluctus`. Then we'll use
   api_importer.rb to import all other data directly through the REST API to
   ensure that all that data (~300,000 records) actually *can* be pushed through
   the REST API. For the live server, all data will be imported through the
   rake task (several million records).


# Demo migration

Change into the Pharos directory, then run this:

```
bundle exec rake pharos:empty_db
bundle exec rake pharos:transition_Fluctus
```

In rails console, run this:

```
admin = User.where(email: 'system@aptrust.org').first
admin.password = 'password'
admin.save
```

Then make sure you can log in as system@aptrust.org at localhost:3000.
