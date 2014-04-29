require 'zip'
require 'nokogiri'
require 'open-uri'

module Nppes
  module UpdatePack
    class Pack
      class << self

        def proceed(zip_file)
          Nppes.logger.warn 'proceed file'
          zip = Zip::File.open(zip_file)
          data = zip.entries.detect {|entry| entry.name =~ /npidata_\d+-\d+\.csv/}

          raise Exception.new('head or data not found') unless data || head

          extracted_filename=File.join(@tmpdirpath, data.name)
          data.extract(extracted_filename) unless File.exists?(extracted_filename)
          data = UpdatePack::Data.new(File.open(extracted_filename), npi_interrupted_at)
          Nppes.logger.warn 'proceed data'
          data.proceed
        end

        def proceed_updates
          Nppes::NpUpdateCheck.proc_needed.each do |update|
            proceed_update(update)
          end
        end

        def background_proceed_updates
          Nppes::NpUpdateCheck.proc_needed.each do |update|
            Delayed::Job.enqueue(Nppes::Jobs::UpdaterJob.new(update))
          end
        end

        def background_init_base
          Delayed::Job.enqueue(Nppes::Jobs::IniterJob.new)
        end


        def init_base
          set_lock_file
          Nppes.logger.warn 'find init file'
          doc = Nokogiri::HTML(open(Nppes.updates_url))
          link = doc.css('a').detect do |link|
            link['href'] =~ Nppes.initiate_signature
          end
          raise Exception.new('Initial file not found') unless link
          proceed(prepare_file(link['href'])) 
        end

        def check_updates
          Nppes.logger.warn 'find updates'
          doc = Nokogiri::HTML(open(Nppes.updates_url))
          signature = Nppes.weekly ? Nppes.weekly_signature : Nppes.monthly_signature

          doc.css('a').each do |link|
            Nppes::NpUpdateCheck.where(file_link: link['href']).first_or_create if link['href'] =~ signature
          end

          proceed_updates
        end

        def background_check_updates(continious = false)
          Delayed::Job.enqueue(Nppes::Jobs::SearcherJob.new((Nppes.get_time_period if continious)))
        end

        def proceed_update(update)
          begin
            proceed(prepare_file(update.file_link))
          rescue
            Nppes.logger.error $!
            Nppes.logger.error $@
            update.update_attribute(:done, false)
          else
            update.update_attribute(:done, true)
          end
        end

        protected
        def prepare_file(file_link)
          @tmpdirpath=File.join(Dir.pwd,'tmpnpi')
          Dir.mkdir(@tmpdirpath) unless Dir.exists? @tmpdirpath
          unless @init_lock_exists 
            ret_file = open(file_link)
            file=File.open File.join(@tmpdirpath, File.basename(file_link)), 'w+' 
            file << ret_file.read.force_encoding('utf-8')
            file.flush
            File.open(@init_lock_path,'w+'){|lock| lock.puts file.path}
            prepared_file_path=file.path
          else
            prepared_file_path=File.open(@init_lock_path,'r'){|lock|lock.gets.chomp}
          end
          Nppes.logger.warn 'prepare file'
          prepared_file_path
        end

        def npi_interrupted_at
          last_record=NpIdentifier.last
          if last_record
            puts "Possible interruption, continuing from the last record in the database: NPI=#{last_record.npi}"
            last_record.npi
          else
            nil
          end
        end

        def set_lock_file
          @init_lock_path=File.join(Dir.pwd,'initbase.lock')
          @init_lock_exists=File.exists?(@init_lock_path)
        end


      end
    end
  end
end
