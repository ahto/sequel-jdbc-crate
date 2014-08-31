
# Examples taken mostly from
# http://sequel.jeremyevans.net/rdoc/files/README_rdoc.html
# to see how much features the driver can do compared to mysql and postgresql

##############
# Connecting #
##############

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

## NOT SUPPORTED: ERROR -- : Java::IoCrateActionSql::SQLActionException: Unsupported statement.: SELECT count(*) AS "count" FROM (select id from items) AS "t1" LIMIT 1
##You can also create datasets based on raw SQL:
##dataset = DB['select id from items']
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
DB.create_table :posts do
  primary_key :id
  String :name
  String :title
  String :category
  String :author
  Date :date
  String :state
  Timestamp :stamp
  Integer :backup_number
  Integer :num_comments
end

posts = DB.from(:posts)
posts = DB[:posts] # same

posts.insert(:id => SecureRandom.uuid,
             :name => 'abc',
             :date => Time.now.utc.to_date,
             :category => 'linux',
             :author => 'JKR',
             :stamp => Time.now)
DB.run('REFRESH TABLE posts')

######################
# Retrieving Records #
######################

#If the dataset is ordered, you can also ask for the last record:
p posts.order(:stamp).last

######################
# Filtering Records #
######################

posts.where(:stamp => (Date.today - 14)..(Date.today + 1)).each {|post| p post[:stamp] }

posts.where(:category => ['ruby', 'postgres', 'linux']).each {|post| p post[:category] }

posts.where{stamp > Date.today << 1}.each {|post| p post[:stamp] }

##this doesnt work yet but maybe it can be made to work
##fails with Sequel::InvalidOperation: Pattern matching via regular expressions is not supported on
## maybe will work with this https://crate.io/docs/stable/sql/analyzer.html?highlight=regular#pattern
#posts.where(:category => /linux/i).each {|post| p post[:stamp] }


posts.exclude(:category => ['ruby', 'postgres', 'linux']).each {|post| p post[:category] }

posts.where('stamp IS NOT NULL').first

author_name = 'JKR'
posts.where('(stamp < ?) AND (author != ?)', Date.today - 3, author_name).first

## NOT SUPPORTED ERROR -- : Java::IoCrateActionSql::SQLActionException: Unsupported statement.: SELECT * FROM "items" WHERE (price > (SELECT (avg("price") + 100) FROM "items")) LIMIT 1
# DB[:items].where('price > ?', DB[:items].select{avg(price) + 100}).first


#######################
# Summarizing Records #
#######################

posts.where(Sequel.like(:category,'%ruby%')).count

puts items.max(:price)
puts items.min(:price)
puts items.sum(:price)
puts items.avg(:price)

####################
# Ordering Records #
####################

posts.order(:stamp).all
posts.order(:stamp, :name).all
posts.order(:stamp).order_prepend(:name).all
posts.reverse_order(:stamp).all
posts.order(Sequel.desc(:stamp)).all

#####################
# Selecting Columns #
#####################

posts.select(:stamp).all
posts.select(:stamp, :name).all
posts.select(:stamp).select_append(:name)

####################
# Deleting Records #
####################

posts.where('stamp < ?', Date.today - 3).delete

#####################
# Inserting Records #
#####################

posts.insert(:id => SecureRandom.uuid, :category => 'ruby', :author => 'jeremy')

####################
# Updating Records #
####################

posts.where('stamp < ?', Date.today - 7).update(:state => 'archived')

# Didnt work. ERROR -- : Java::IoCrateActionSql::SQLActionException: Validation failed for backup_number: Invalid value of type 'FUNCTION': UPDATE "posts" SET "backup_number" = ("backup_number" + 1) WHERE ("stamp" < '2014-08-24')
#posts.where{|o| o.stamp < Date.today - 7}.update(:backup_number => Sequel.+(:backup_number, 1))

################
# Transactions #
################

#This works but doesn't actually use transactions. Because crate doesn't support them.
DB.transaction do
  posts.insert(:id => SecureRandom.uuid, :category => 'ruby', :author => 'jeremy')
  posts.where('stamp < ?', Date.today - 7).update(:state => 'archived')
end

##################
# Joining Tables #
##################

#Nope

###############################
# Column references in Sequel #
###############################

#Java::JavaLang::IllegalArgumentException: sizes columns and types do not match
#items.where(:x => 1).all

#Java::JavaLang::IllegalArgumentException: sizes columns and types do not match
#items.where(1 => :x).all

###############################################
# Qualifying identifiers (column/table names) #
###############################################

puts items.literal(:items__price)

puts items.literal(Sequel.qualify(:items, :price))

# This doesn't make sense here, crate doesn't have databases, only tables.
#DB[:some_schema__posts]

######################
# Identifier aliases #
######################

puts items.literal(:price___p)
puts items.literal(:items__price___p)
puts items.literal(Sequel.as(:price, :p))
puts items.literal(Sequel.as(DB[:posts].select{max(id)}, :p))

#################
# Sequel Models #
#################

class Post < Sequel::Model
end

puts Post.table_name

###################
# Model instances #
###################

uuid = SecureRandom.uuid
posts.insert(:id => uuid, :category => 'ruby', :title => 'hello world', :author => 'jeremy')
DB.run('REFRESH TABLE posts')

post = Post[uuid]
puts post.pk


# doesnt work now. look into this. it does correct query but results in
# NoMethodError: undefined method `pk' for nil:NilClass
class PostWithCompositeKey < Sequel::Model(:posts)
  set_primary_key [:category, :title]
end
post_with_composite_key = PostWithCompositeKey['ruby', 'hello world']
puts post_with_composite_key.pk


puts Post[:title => 'hello world']
puts Post.first{num_comments < 10}

#######################
# Acts like a dataset #
#######################

Post.where(:category => 'ruby').each{|post| p post}

Post.where{num_comments < 7}.delete
Post.where(Sequel.like(:title, 'ruby')).update(:category => 'ruby')

###########################
# Accessing record values #
###########################

puts post.values
puts post.id
puts post.title

puts post[:id]
puts post[:title]

post.title = 'hey there'
post[:title] = 'hey there'
post.save

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