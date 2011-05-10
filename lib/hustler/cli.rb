require 'thor'

module Juggler
  class CLI < Thor
    desc "work", "Starts watching incoming S3 queue for files to process"

    def work
      puts "Watching for incoming files in bucket '#{Juggler.config['queue_bucket_name']}'..."
      loop do
        Juggler::Worker.run
        sleep 1
      end
    end
  end
end
