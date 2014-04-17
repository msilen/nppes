module Nppes
module UpdatePack
  class Data < UpdatePack::Base
    def initialize(data_file)
      @file = data_file
      @existing_npis=Nppes::NpIdentifier.pluck(:npi) #array of integers
    end

    def set_or_clear_batches
      @npi_batch, @npi_address_batch, @npi_license_batch=[],[],[]
    end

    def proceed
      @batch_size_limit=1000
      batch_timer_checkset
      set_or_clear_batches
      parse(@file) do |row|
        if @npi_batch.size >= @batch_size_limit 
          puts "customTimer: #{@cumulative}";@cumulative=0
          print_info;sleep 5
          puts @npi_address_batch.size
          puts @npi_license_batch.size
          NpIdentifier.import @npi_batch
          form_npi_association_batch #now @npi_license_batch and npi_address_batch are modified to be imported
          NpLicense.import @npi_license_batch
          NpAddress.import @npi_address_batch
          set_or_clear_batches
          #oreak
        end
        processed_row=proceed_row(row)
        @npi_batch << processed_row if processed_row  #packing npi_batch
      end
     end

    def form_npi_association_batch
      resulted_ids=NpIdentifier.limit(@batch_size_limit).order('id desc').pluck(:id,:npi)#getting actual ids after import
      [@npi_license_batch, @npi_address_batch].each do |association|
        association.map! do |ass|
          cur_npi=ass.last
          cur_id=resulted_ids.rassoc(cur_npi).first
          ass.first.np_identifier_id=cur_id
          ass.first
        end
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

      #filling main model record(NpIdentifier)
      required_fields.fields.each_pair { |k, v| nppes_record.send("#{k}=", prepare_value(@fields, v)) }
      # for submodels
      #s - submodels f -fields with indexes s :np_addresses | :np_licenses
      #f -> [{:address_type=>"official", :address1=>28,:zip=>32}, {:address_type=>"mailing", :address1=>20,...}]
      required_fields.relations.each_pair do |s, f|
        f.each do |entity|
          #relation = nppes_record.send(s).new
          v_f=get_validity_fields(s,entity)
          relation = s.constantize.new
          subset=(v_f-non_empty_fields_indexes).empty?
          #byebug
          if subset#check if non_empty_fields_indexes contain all required fields
          timer_on;@cumulative||=0
            entity.each_pair {|name, num| relation.send("#{name}=", prepare_value(@fields, num))}
            relation.valid?
          @cumulative+=timer_off
            unless relation.valid?
              #nefa=non_empty_fields_indexes
              byebug unless (nefa & entity.values).blank?
              #не создавать сущности и заполнять пустые поля - лицензии к примеру
              #nppes_record.send(s).delete(relation)
              break
            end
          else
            next
          end
          add_to_relevant_batch(s,relation,current_npi)
        end
      end
    #  nppes_record.save if nppes_record.valid?
      nppes_record
    end

    protected

    def get_validity_fields(submodel,fields_and_positions)
      #f_and_p: {:taxonomy_code=>47,... :healthcare_taxonomy_switch=>50}
      validity_fields = {
        "Nppes::NpAddress" => [:address1,:city,:state,:country,:zip],
        "Nppes::NpLicense" => [:license_number,:taxonomy_code]
      }
      return fields_and_positions.values_at(*validity_fields[submodel])
    end

    def add_to_relevant_batch(submodel,relation,current_npi)
      if submodel=="Nppes::NpAddress"
        @npi_address_batch<<[relation,current_npi]
      else
        @npi_license_batch<<[relation,current_npi]
      end
    end

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
      records_total+=@batch_size_limit
      puts "elapsed: #{@seconds_for_previous_batch}"
      puts "total_records: #{records_total}"
    end
    end
  end
end
