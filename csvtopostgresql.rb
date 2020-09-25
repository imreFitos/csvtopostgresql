#!/usr/bin/env ruby
#
# take a CSV file with a header and create a table and populate with all the data
#
# imre Fitos, 2018

require 'csv'
require 'pg'

args = ARGV.join(' ')

puts "#{$0} #{args} started."

unless ENV["DATABASE_URL"]
  puts "#{$0} missing DATABASE_URL environment variable, aborted."; exit 1
end

url = ENV["DATABASE_URL"].split(/[\/:@]/)
(puts "#{$0} missing db name from DATABASE_URL, aborted."; exit 1) unless dbname = url[6]
(puts "#{$0} missing host name from DATABASE_URL, aborted."; exit 1) unless host = url[5]
(puts "#{$0} missing username from DATABASE_URL, aborted."; exit 1) unless password = url[4]
(puts "#{$0} missing password from DATABASE_URL, aborted."; exit 1) unless user = url[3]

unless ARGV.length == 2
  puts "usage: #{$0} [csv-file-to-upload] [table-to-create-and-upload-to]. aborted."; exit 1
end

unless File.exist?(ARGV[0])
  puts "#{$0} Cannot find #{ARGV[0]}, aborted."; exit 1
end

count = 0
begin
  conn = PG.connect(:host => host, :user => user, :password => password, :dbname => dbname)
  conn.exec "set client_min_messages = warning"
  conn.set_client_encoding('unicode')
  conn.exec "DROP TABLE IF EXISTS #{ARGV[1]}"

  File.open(ARGV[0], 'r:UTF-8') do |file|
    csv = CSV.new(file, headers: false, encoding: "UTF-8")
    headers = csv.shift

    createtable = "CREATE TABLE #{ARGV[1]} ("
    headers.each { |h|
      h.gsub!(/[\/: \\?]/, '_')
      createtable << "\"#{h}\" TEXT, "
    }
    createtable.chomp!(", ")
    createtable << ");"

    conn.exec createtable

    enco = PG::TextEncoder::CopyRow.new
    conn.copy_data "COPY #{ARGV[1]} FROM STDIN", enco do
      while row = csv.shift
        conn.put_copy_data row
        count += 1
      end
    end
  end

rescue PG::Error => e
  puts e.message
  puts "#{$0} aborted."
  exit 1
ensure
  conn.close if conn
end
puts "#{$0} #{args} inserted #{count} rows."
puts "#{$0} #{args} finished."
