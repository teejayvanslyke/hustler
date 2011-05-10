require 'thor'

module Hustler
  class CLI < Thor
    desc "work", "Starts watching incoming S3 queue for files to process"

    def work
      puts "Watching for incoming files in bucket '#{Hustler.config['queue_bucket_name']}'..."
      loop do
        Hustler::Worker.run
        sleep 1
      end
    end
  end
end
