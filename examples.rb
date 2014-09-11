
# Examples taken mostly from
# http://sequel.jeremyevans.net/rdoc/files/README_rdoc.html
# to see how much features the driver can do compared to mysql and postgresql

##############
# Connecting #
##############
require 'securerandom'
require 'logger'
require 'sequel'
require 'sequel/adapters/jdbc/crate'

DB = Sequel.connect('jdbc:crate://localhost:4300')
DB.loggers << Logger.new($stdout)

###########
# Helpers #
###########
def safe_drop(table)
  DB.execute(%Q(DROP TABLE "#{table}"))
rescue Sequel::DatabaseError
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
  String :updated_by
  Date :date
  String :state
  Timestamp :stamp
  Integer :backup_number
  Integer :num_comments
  Boolean :visible
  Date :written_on
end

posts = DB.from(:posts)
posts = DB[:posts] # same

posts.insert(:id => SecureRandom.uuid,
             :name => 'abc',
             :date => Time.now.utc.to_date, # Dates need to be converted to utc first manually, if you care about it.
             :category => 'linux',
             :author => 'JKR',
             :stamp => Time.now) # Times don't need to be in utc. crate handles the conversion internally.
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

items.where(:x => 1).all

items.where(1 => :x).all

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
  def before_create
    self.id = SecureRandom.uuid
    super
  end

  def after_create
    super
    puts 'After create hook called'
  end

  def after_destroy
    super
    puts 'After destroy hook called'
  end
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

###################
# Mass assignment #
###################

post.set(:title=>'hey there', :updated_by=>'foo')
post.save

post.update(:title => 'hey there', :updated_by=>'foobar')

########################
# Creating new records #
########################

post = Post.create(:title => 'hello world')

post = Post.new
post.title = 'hello world'
post.save

post = Post.new do |p|
  p.title = 'hello world'
end

post = Post.create{|p| p.title = 'hello world'}

#########
# Hooks #
#########

#example in the Post model

####################
# Deleting records #
####################

begin
  post = Post.first
  post.delete # => bypasses hooks
  post = Post.last
  post.destroy # => runs hooks
rescue Sequel::NoExistingObject
  #No worries
end

Post.where(:category => 'mysql').delete # => bypasses hooks
Post.where(:category => 'ruby').destroy # => runs hooks

################
# Associations #
################

#Hmm

#################
# Eager Loading #
#################

# Double hmm

#############################
# Joining with Associations #
#############################

# not gonna happen

####################################
# Extending the underlying dataset #
####################################

class PostWithDatasetMethods < Sequel::Model(:posts)
  dataset_module do
    def clean_posts_with_few_comments
      posts_with_few_comments.delete
    end
  end

  subset(:posts_with_few_comments){num_comments < 30}
  subset :invisible, Sequel.~(:visible)
end

PostWithDatasetMethods.where(:category => 'ruby').clean_posts_with_few_comments
PostWithDatasetMethods.where(:category => 'ruby').invisible.all

#####################
# Model Validations #
#####################

class PostWithValidation < Sequel::Model(:posts)
  def validate
    super
    errors.add(:name, "can't be empty") if name.nil? || name.empty?
    errors.add(:written_on, "should be in the past") if written_on.nil? || written_on >= Time.now
  end
end

begin
  PostWithValidation.create(:title => 'hello world')
rescue Sequel::ValidationFailed
  puts 'Yay, validation worked'
end
