module Nppes
  module UpdatePack
    class Data < UpdatePack::Base
      def initialize(data_file)
        @file = data_file
      end

      def proceed
        npi_batch=[]
        records_total=0
        previous_timemark=Time.now

        @already_exists=Nppes::NpIdentifier.pluck(:npi) #array of integers

        parse(@file) do |row|
          if npi_batch.size >= 1000
            records_total+=1000
            puts "elapsed: #{Time.now - previous_timemark}"
            previous_timemark=Time.now
            puts "total_records: #{records_total}"
            NpIdentifier.import npi_batch
            npi_batch.clear
            #break
          end
          processed_row=proceed_row(row)
          npi_batch << processed_row if processed_row  #packing npi_batch
        end
       end

      def proceed_row(row, required_fields = RequiredFields)
        @fields = split_row(row)
        npi = Nppes::NpIdentifier.where(npi: @fields[0]).first_or_initialize

        unless @already_exists.include? @fields[0].to_i #old method first_or_initialize won't work because sql query creates, not update records.
          npi = Nppes::NpIdentifier.new(npi: @fields[0])
        else
          return nil
        end

        required_fields.fields.each_pair { |k, v| npi.send("#{k}=", prepare_value(@fields, v)) }

        # for submodels
        required_fields.relations.each_pair do |k, v|
          v.each do |entity|
            relation = npi.send(k).new
            entity.each_pair {|name, num| relation.send("#{name}=", prepare_value(@fields, num))}
            unless relation.valid?
              npi.send(k).delete(relation)
              break
            end
          end
        end
      #  npi.save if npi.valid?
        npi
      end

      protected
        def prepare_value(fields, variants)
          if variants.is_a? String
            result=variants
          elsif variants.is_a? Array
            variant = variants.detect {|v| fields[v].present? }
            result=variant ? fields[variant] : ''
          else
            result=fields[variants]
          end

          if result.is_a?(String) && result.scan(/\d\d\/\d\d\/\d\d\d\d/).present?
            result=Date.strptime result, "%m/%d/%Y"
          end
          result.blank? ? nil: result
        end
    end
  end
end
