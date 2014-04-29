module Nppes
module UpdatePack
  class Data < UpdatePack::Base
    def initialize(data_file,npi_interrupted_at=nil)
      @npi_interrupted_at=npi_interrupted_at
      @file = data_file
      @batch_size_limit=1000
    end

    #NpIdentifier.last.exists? puts "records exists, continue starts from the last record in the database" blah

    def set_or_clear_batches
      @npi_batch, @npi_address_batch, @npi_license_batch=[],[],[]
    end

    def proceed
      batch_timer_check
      set_or_clear_batches
      parse(@file) do |row|
        if @npi_batch.size >= @batch_size_limit 
          print_info
          NpIdentifier.import @npi_batch
          assign_ids_to_npi_association_batch 
          NpLicense.import @npi_license_batch
          NpAddress.import @npi_address_batch
          set_or_clear_batches
        end
        @fields = split_row(row)
        @curnpi=@fields.first.to_i

        if @npi_interrupted_at
          next unless found_last_npi?(@curnpi)
        end
        processed_row=proceed_row(row)
        @npi_batch << processed_row if processed_row  #packing npi_batch
      end
     end

    def assign_ids_to_npi_association_batch
      resulted_ids=NpIdentifier.limit(@batch_size_limit).order('id desc').pluck(:id,:npi)#getting actual ids after import
      [@npi_license_batch, @npi_address_batch].each do |association|
        association.map! do |ass|
          cur_processed_npi=ass.last
          cur_id=resulted_ids.rassoc(cur_processed_npi).first
          ass.first.np_identifier_id=cur_id
          ass.first
        end
      end
    end


    def proceed_row(row, required_fields = RequiredFields)
      nefia=non_empty_fields_indexes
      nppes_record = Nppes::NpIdentifier.new(npi: @curnpi)

      #filling main model record(NpIdentifier)
      required_fields.fields.each_pair { |k, v| nppes_record.send("#{k}=", prepare_value(@fields, v)) }
      # for submodels
      #s - submodels f -fields with indexes s :np_addresses | :np_licenses
      #f -> [{:address_type=>"official", :address1=>28,:zip=>32}, {:address_type=>"mailing", :address1=>20,...}]
      required_fields.relations.each_pair do |s, f|
        f.each do |entity|
          #relation = nppes_record.send(s).new
          timer_on
          v_f=get_validity_fields(s,entity) #v_f=validity_fields
          relation = s.constantize.new
          subset=(v_f-nefia).empty?
          timer_off
          if subset#check if non_empty_fields_indexes contain all required fields
            entity.each_pair {|name, num| relation.send("#{name}=", prepare_value(@fields, num))}
            unless relation.valid?
              byebug
            end
          else
            next
          end
          add_to_relevant_batch(s,relation,@curnpi)
        end
      end
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

    def timer_on(options={})
      if options[:reset]
        @cumulative=0;@icount=0
        return
      end
      @cumulative||=0;@icount||=0
      @mark_one=Time.now
    end

    def timer_off
      result=Time.now-@mark_one
      @cumulative+=result;@icount+=1
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

    def batch_timer_check
      @seconds_for_previous_batch=Time.now-@last_time_mark if @last_time_mark
      @last_time_mark = Time.now
    end

    def found_last_npi?(curnpi)
      @searchstarted||=Time.now
      @ncount ||=0
      unless @npi_interrupted_at==curnpi
        @ncount +=1
        puts @ncount if @ncount%10000==0
        return false
      else
        @searchtime=Time.now-@searchstarted
        puts "Time passed: #{@searchtime} sec."
        puts "Skipped to last npi, continuing..."
        sleep 5
        @npi_interrupted_at=nil
        return true
      end
    end

    def print_info
      batch_timer_check
      @records_total ||= 0
      @records_total+=@batch_size_limit
      puts "elapsed: #{@seconds_for_previous_batch}"
      puts "total_records: #{@records_total}"
      puts "customTimer: #{@cumulative}, count: #{@icount}"
      timer_on(:reset => true)
      puts "addresses imported: #{@npi_address_batch.size}"
      puts "licenses imported: #{@npi_license_batch.size}"
    end
    end
  end
end
