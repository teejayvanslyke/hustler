$:.unshift File.join(File.dirname(__FILE__),'..','lib')

require 'rubygems'
require 'bundler'
require 'digest/sha1'

Bundler.require

require 'juggler/cli'

module Juggler

  def self.config
    @config || YAML.load_file(File.dirname(__FILE__) + '/../config/s3.yml')['development']
  end

  def configure(options)
    @config = options
  end

  def self.queue_bucket
    AWS::S3::Bucket.find(self.config['queue_bucket_name'])
  end

  def self.processed_bucket
    AWS::S3::Bucket.find(self.config['processed_bucket_name'])
  end

  def self.queue
    queue_bucket.objects.reject {|o| o.path.include?("---LOCKED---")}
  end

  def self.processor=(processor)
    @processor = processor
  end
  
  def self.processor
    @processor
  end

  class Worker

    def self.run
      new.run
    end

    def initialize
    end

    def run
      Juggler.queue.each do |object|
        Juggler::Job.run(object)
      end
    end

  end

  class Job
    def self.run(object)
      object = lock(object)
      if (result = perform(StringIO.new(object.value)))
        cleanup object
        to_write = result.is_a?(Array) ? result : [ result ]
        to_write.each do |io|
          AWS::S3::S3Object.store(sha1(io), io, Juggler.config['processed_bucket_name'])
        end
      end
    end

    def self.sha1(io)
      sha1 = Digest::SHA1.new
			counter = 0
			while (!io.eof)
				buffer = io.readpartial($BUFLEN)
				sha1.update(buffer)
			end
      return sha1.hexdigest
    end

    def self.lock(object)
      parts = File.split(object.path)
      name = "---LOCKED---#{parts.last}"
      object.rename(name)
      Juggler.queue_bucket[name]
    end

    def self.perform(io)
      Juggler.processor.run(io)
    end

    def self.cleanup(object)
      object.delete
    end

    def initialize(object)
    end
  end

  class PassthroughProcessor
    def run(io)
      io
    end
  end

end

Juggler.processor = Juggler::PassthroughProcessor.new # by default

AWS::S3::Base.establish_connection!(
  :access_key_id     => Juggler.config['access_key_id'],
  :secret_access_key => Juggler.config['secret_access_key']
)

