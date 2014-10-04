
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
DB.run('REFRESH TABLE posts')

#################
# Sequel Models #
#################

class Post < Sequel::Model
  def before_create
    self.id = SecureRandom.uuid
    super
  end
end

###################
# Model instances #
###################

post = Post.create(:title => 'hello world')

DB.run('REFRESH TABLE posts')

post = Post.first
puts post.pk

