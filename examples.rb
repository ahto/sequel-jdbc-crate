require 'logger'
require 'sequel'
require 'sequel/adapters/jdbc/crate'

DB = Sequel.connect('jdbc:crate://localhost:4300')
DB.loggers << Logger.new($stdout)

##############
# Time setup #
##############
# require 'active_support/core_ext' # for time calculations in this examples.rb file
# Time.zone = 'UTC'
#Sequel.default_timezone = :utc

# class ZoneAwareTime
#   def self.parse(*args)
#     Time.zone.parse(*args)
#   end
#
#   def self.now
#     Time.current
#   end
#
#   def self.current
#     Time.current
#   end
#
#   def self.new(*args)
#     Time.zone.local(*args)
#   end
# end
#
# Sequel.datetime_class = ZoneAwareTime

###########
# Helpers #
###########
def safe_drop(table)
  DB.execute(%Q(DROP TABLE "#{table}"))
rescue Java::IoCrateActionSql::SQLActionException, NoMethodError
  #no problem, the table didnt exist
end


#################
# Does it work? #
#################

DB["SELECT * FROM sys.nodes"].each { |node| p node[:name] }

###################
# A short example #
###################

safe_drop(:items)
DB.create_table :items do
  primary_key :id
  String :name
  Float :price
end

items = DB[:items] # Create a dataset

# Populate the table
items.insert(:id => SecureRandom.uuid, :name => 'abc', :price => rand * 100)
items.insert(:id => SecureRandom.uuid, :name => 'def', :price => rand * 100)
items.insert(:id => SecureRandom.uuid, :name => 'ghi', :price => rand * 100)
DB.run('REFRESH TABLE items')

# Print out the number of records
puts "Item count: #{items.count}"

# Print out the average price
puts "The average price is: #{items.avg(:price)}"

#########################
# Arbitrary SQL queries #
#########################

#You can execute arbitrary SQL code using Database#run:
DB.run("create table t (a string, b string) with (number_of_replicas = '0-all')")
DB.run("insert into t values ('a', 'b')")
DB.run("drop table t")

# NOT SUPPORTED: ERROR -- : Java::IoCrateActionSql::SQLActionException: Unsupported statement.: SELECT count(*) AS "count" FROM (select id from items) AS "t1" LIMIT 1
##You can also create datasets based on raw SQL:
#dataset = DB['select id from items']
#dataset.count # will return the number of records in the result set
#dataset.map(:id) # will return an array containing all values of the id column in the result set

#You can also fetch records with raw SQL through the dataset:
DB['select * from items'].each do |row|
 p row
end

#You can use placeholders in your SQL string as well:
name = 'abc'
DB['select * from items where name = ?', name].each do |row|
 p row
end

#############################
# Getting Dataset Instances #
#############################

safe_drop(:posts)
# DB.run("create table posts (
# id string PRIMARY KEY,
# name string,
# date timestamp,
# stamp timestamp
# ) with (number_of_replicas = '0-all')")
DB.create_table :posts do
  primary_key :id
  String :name
  Date :date
  Timestamp :stamp
end

posts = DB.from(:posts)
posts = DB[:posts] # same

posts.insert(:id => SecureRandom.uuid, :name => 'abc', :date => Time.now.utc.to_date, :stamp => Time.now.iso8601)
DB.run('REFRESH TABLE posts')

######################
# Retrieving Records #
######################

#If the dataset is ordered, you can also ask for the last record:
p posts.order(:stamp).last

######################
# Filtering Records #
######################

my_posts = posts.where(:stamp => (Date.today - 14)..(Date.today - 7))




# #or just
#
# safe_drop(:locations)
# DB.execute(%q|CREATE TABLE locations (
# id integer,
# name string,
# date timestamp,
# kind string,
# position integer,
# PRIMARY KEY (id)
# ) with (number_of_replicas = '0-all')|)
#
#
#
#
#







# DB_CRATE.execute('REFRESH TABLE transits')

# transits = DB_CRATE[:transits]
# puts "Transits count: #{transits.count}"
#
# begin
#   DB_CRATE.execute('DROP TABLE items')
# rescue
#   #do nothing, the table just didnt exist
# end
#
# # create an items table
# DB_CRATE.create_table :items do
#   primary_key :id
#   String :name
#   Float :price
# end
#
# # create a dataset from the items table
# items = DB_CRATE[:items]
#
# # populate the table
# uuid = SecureRandom.uuid
# items.insert(:id => uuid, :name => 'abc', :price => rand * 100)
# items.insert(:id => SecureRandom.uuid, :name => 'def', :price => rand * 100)
# items.insert(:id => SecureRandom.uuid, :name => 'ghi', :price => 200)
# DB_CRATE.execute('REFRESH TABLE items')
#
# # print out the number of records
# puts "Item count: #{items.count}"
#
# # print out the average price
# puts "The average price is: #{items.avg(:price)}"
#
# DB_CRATE['select * from items'].each do |row|
#   puts row
# end
#
# class Item < Sequel::Model(DB_CRATE)
#   def before_create
#     self.id = SecureRandom.uuid
#     super
#   end
# end
#
# item = Item[uuid]
# puts item.inspect
# puts item.price
#
# puts Item[:name => 'ghi'].inspect
# puts Item.first{price > 199}.inspect
#
# Item.where(:name => 'ghi').each{|item| p item}
#
# puts item.values
# item.price = 500
# item.save
# puts item.inspect
#
# item.update(:name => 'new_abc', :price=>100)
# puts item.inspect
#
# item = Item.create(:name => 'from_model', :price => 1000)
# item.delete # => bypasses hooks
#
# item = Item.new
# item.name = 'another_model'
# item.save
# item.destroy # => runs hooks
#
# Item.where{price < 100}.delete # => bypasses hooks
# Item.where{price >= 100}.destroy # => runs hooks