require 'sequel/adapters/jdbc'

require 'jdbc/crate'
Jdbc::Crate.load_driver

require 'pp'

module Sequel
  module JDBC
    Sequel.synchronize do
      DATABASE_SETUP[:crate] = proc do |db|
        db.extend(Sequel::JDBC::Crate::DatabaseMethods)
        db.extend_datasets(Sequel::JDBC::Crate::DatasetMethods)

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

        #iso8601

        # default behaviour will mangle timezones
        def to_application_timestamp(v)
          #TODO: triple check this
          Time.local(*v)
        end

        def application_to_database_timestamp(v)
          convert_output_timestamp(v, Sequel.database_timezone)
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

        #this isn't called but needed so that sequel will do schema parsing
        #sequel asks: respond_to?(:schema_parse_table, true)
        def schema_parse_table(table_name, opts)
        end

        #crate doesn't support transactions
        def begin_transaction(*args); end
        def commit_transaction(*args); end
        def rollback_transaction(*args); end



      end

      module DatasetMethods

      end

    end
  end
end
