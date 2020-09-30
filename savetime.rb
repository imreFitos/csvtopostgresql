#!/usr/bin/env ruby
#
# add a tablename and a timestamp to the given table, generally import_logs
#
# imre Fitos, 2018

require 'pg'

args = ARGV.join(' ')

unless ENV["DATABASE_URL"]
  puts "#{$0} missing DATABASE_URL environment variable, aborted."; exit 1
end

url = ENV["DATABASE_URL"].split(/[\/:@]/)
(puts "#{$0} missing db name from DATABASE_URL, aborted."; exit 1) unless dbname = url[6]
(puts "#{$0} missing host name from DATABASE_URL, aborted."; exit 1) unless host = url[5]
(puts "#{$0} missing username from DATABASE_URL, aborted."; exit 1) unless password = url[4]
(puts "#{$0} missing password from DATABASE_URL, aborted."; exit 1) unless user = url[3]

unless ARGV.length == 2
  puts "usage: #{$0} [import_logs table name] [table name to timestamp]. aborted."; exit 1
end

count = 0
begin
  conn = PG.connect(:host => host, :user => user, :password => password, :dbname => dbname)
  conn.exec "set client_min_messages = warning"
  conn.set_client_encoding('unicode')
  conn.exec "CREATE TABLE IF NOT EXISTS #{ARGV[0]} (table_name VARCHAR UNIQUE, loaded_at TIMESTAMP)"
  conn.exec "INSERT INTO #{ARGV[0]} (table_name, loaded_at) VALUES ('#{ARGV[1]}', now()) ON CONFLICT (table_name) DO UPDATE SET loaded_at=now()"

rescue PG::Error => e
  puts e.message
  puts "#{$0} aborted."
  exit 1
ensure
  conn.close if conn
end
puts "#{ARGV[0]} updated with #{ARGV[1]}"
