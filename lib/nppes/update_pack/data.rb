module Nppes
module UpdatePack
  class Data < UpdatePack::Base
    def initialize(data_file)
      @file = data_file
      @existing_npis=Nppes::NpIdentifier.pluck(:npi) #array of integers
    end

    def proceed
      batch_timer_checkset
      npi_batch=[]
      parse(@file) do |row|
        if npi_batch.size >= 1000
          puts "split_row #{@cumulative}";@cumulative=0
          print_info;sleep 5
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
      current_npi=@fields[0].to_i

      unless @existing_npis.include? current_npi #old method first_or_initialize won't work because sql query creates, not update records.
        nppes_record = Nppes::NpIdentifier.new(npi: current_npi)
      else
        return nil
      end

        timer_on;@cumulative ||=0
        byebug
      required_fields.fields.each_pair { |k, v| nppes_record.send("#{k}=", prepare_value(@fields, v)) }
        @cumulative+=timer_off;
      # for submodels
      #s - submodels f -fields with indexes s :np_addresses | :np_licenses
      #f -> [{:address_type=>"official", :address1=>28,:zip=>32} {:address_type=>"mailing", :address1=>20,...}]
      required_fields.relations.each_pair do |s, f|
        f.each do |entity|
          relation = nppes_record.send(s).new
          entity.each_pair {|name, num| relation.send("#{name}=", prepare_value(@fields, num))}
          unless relation.valid?
            nefa=non_empty_fields_indexes
            #byebug unless (nefa & entity.values).blank?
            #не создавать сущности и заполнять пустые поля - лицензии к примеру
            nppes_record.send(s).delete(relation)
            break
          end
        end
      end
    #  nppes_record.save if nppes_record.valid?
      nppes_record
    end

    protected

    def timer_on
      @mark_one=Time.now
    end

    def timer_off
      result=Time.now-@mark_one
      result
    end

    def non_empty_fields_indexes
      result=[]
      @fields.each_with_index{|str,index| result << index unless str.blank?}
      result
    end

    def prepare_value(fields, variants)
      if variants.is_a? String
        result=variants
      elsif variants.is_a? Array
        variant = variants.detect {|v| fields[v].present? }
        result=variant ? fields[variant] : ''
      else
        result=fields[variants]
      end

      if result.is_a?(String) && (date_scan=result.scan(/\d\d\/\d\d\/\d\d\d\d/)).present?
        result=Date.strptime date_scan.first, "%m/%d/%Y"
      end
      result.blank? ? nil: result #empty string "" brake postgresql import, should return nil instead
    end

    def batch_timer_checkset
      @seconds_for_previous_batch=Time.now-@last_time_mark if @last_time_mark
      @last_time_mark = Time.now
    end

    def print_info
      batch_timer_checkset
      records_total ||= 0
      records_total+=1000
      puts "elapsed: #{@seconds_for_previous_batch}"
      puts "total_records: #{records_total}"
    end
    end
  end
end
