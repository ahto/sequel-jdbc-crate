require 'sequel/adapters/jdbc'

require 'jdbc/crate'
Jdbc::Crate.load_driver

module Sequel
  module JDBC
    Sequel.synchronize do
      DATABASE_SETUP[:crate] = proc do |db|
        db.extend(Sequel::JDBC::Crate::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::Crate::Dataset

        #table names in crate are always downcased unless you double-quote them. so Table gets table, but "Table" remains Table
        #sequel will quote table names so dont do anything to it
        db.identifier_input_method = nil

        Jdbc::Crate::Driver::CrateDriver
      end
    end

    # Database and Dataset instance methods for Crate specific
    # support via JDBC.
    module Crate
      # Database instance methods for Crate databases accessed via JDBC.
      module DatabaseMethods

        #this is for when fetching rows and want to convert
        # def setup_type_convertor_map
        #   @type_convertor_map = TypeConvertor::MAP.merge(Java::JavaSQL::Types::TIMESTAMP=>timestamp_convertor)
        #   @basic_type_convertor_map = TypeConvertor::BASIC_MAP
        # end

        # default behaviour will mangle timezones
        def to_application_timestamp(v)
          #TODO: triple check this
          Time.local(*v)
        end

        # # there is no AUTOINCREMENT
        def serial_primary_key_options
          {:primary_key => true, :type=>:String}
        end

        #only one string type called 'string'
        def type_literal_generic_string(column)
          :string
        end

        #crate supports integer, long, short, double, float and byte
        def type_literal_generic_numeric(column)
          :double
        end

        def type_literal_generic_float(column)
          :double
        end

        def type_literal_generic_bignum(column)
          :integer
        end

        def type_literal_generic_integer(column)
          :integer
        end

        #timestamp is only time related type
        def type_literal_generic_date(column)
          :timestamp
        end
        def type_literal_generic_datetime(column)
          :timestamp
        end
        def type_literal_generic_timestamp(column)
          :timestamp
        end


        #Sequel error wrapper will swallow exceptions without this
        def database_error_classes
          [Java::IoCrateActionSql::SQLActionException]
        end

        #Let sequel know what the primary keys are in a table
        def schema_parse_table(table, opts=OPTS)
          primary_key_name = DB["SELECT constraint_name from "\
            "information_schema.table_constraints where "\
            "schema_name='doc' and constraint_type='PRIMARY_KEY' "\
            "and table_name=? limit 1", table].first.fetch(:constraint_name).array.first.to_sym

          sch = super
          sch.each do |c, s|
            if c == primary_key_name
              s[:primary_key] = true
            end
          end
          sch
        end

        #crate doesn't support transactions
        def begin_transaction(*args); end
        def commit_transaction(*args); end
        def rollback_transaction(*args); end

      end

      module DatasetMethods
        # crate needs times in iso8601 format
        def literal_time_append(sql, t)
          literal_string_append(sql, t.iso8601)
        end

        #From: SELECT count(*) AS "count" FROM "posts" WHERE ("category" LIKE '%ruby%' ESCAPE '\') LIMIT 1
        #To: SELECT count(*) AS "count" FROM "posts" WHERE ("category" LIKE '%ruby%') LIMIT 1
        def complex_expression_sql_append(sql, op, args)
          sql = super
          sql.gsub!(" ESCAPE '\\'",'')
        end

      end

      # Dataset class for Crate datasets accessed via JDBC.
      class Dataset < JDBC::Dataset
        include Sequel::JDBC::Crate::DatasetMethods

      end

    end
  end
end
