# sequel-jdbc-crate

Adapter for Sequel using the JDBC driver for connecting to Crate.

## Usage

Example how to connect

    require 'sequel'
    require 'logger'
    DB = Sequel.connect('jdbc:crate://localhost:4300')
    DB.loggers << ::Logger.new($stdout)

## Examples

Please take a look at the [examples.rb](examples.rb) file for all the examples. 
If you get `Java::IoCrateActionSql::SQLActionException: org.elasticsearch.index.shard.IllegalIndexShardStateException`
then just run the command again.

To run the examples with RVM

    $ rvm jruby@sequel-jdbc-crate --create
    $ bundle install --path vendor --binstubs
    $ bundle exec ruby -Ilib examples.rb
    
## Exceptions

Currently, raised exception class is Java::IoCrateActionSql::SQLActionException

    def safe_drop(table)
      DB.execute(%Q(DROP TABLE "#{table}"))
    rescue Java::IoCrateActionSql::SQLActionException
      #no problem,the table didnt exist
    end

## License

Crate JDBC is distributed under the Apache License 2.0, see *LICENSE.txt*.
