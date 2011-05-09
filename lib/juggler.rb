$:.unshift File.join(File.dirname(__FILE__),'..','lib')

require 'rubygems'
require 'bundler'
require 'digest/sha1'
require 'json'

Bundler.require

require 'juggler/cli'

module Juggler

  def self.redis
    @redis ||= Redis.new(:host => @config['redis_host'], :port => @config['redis_port'])
  end

  def self.config
    @config 
  end

  DEFAULT_OPTIONS =
    {
      'redis_host' => 'localhost',
      'redis_port' => 6379
    }.freeze

  def self.configure(options)
    @config = DEFAULT_OPTIONS.merge(options)
    AWS::S3::Base.establish_connection!(
      :access_key_id     => @config['access_key_id'],
      :secret_access_key => @config['secret_access_key']
    )
  end

  def self.queue_bucket
    AWS::S3::Bucket.find(self.config['queue_bucket_name'])
  end

  def self.processed_bucket
    AWS::S3::Bucket.find(self.config['processed_bucket_name'])
  end

  def self.processed_url(id)
    "http://#{self.config['processed_bucket_name']}.s3.amazonaws.com/#{id}"
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
      job = new(object)
      job.run
      job
    end

    def self.find(id)
      new(id)
    end

    def initialize(id_or_object)
      if id_or_object.is_a?(String)
        @id = id_or_object
      else
        @object = lock(id_or_object)
      end

      self.status = 'queued'
    end

    def run
      if (result = perform(StringIO.new(@object.value)))
        cleanup @object
        to_write = result.is_a?(Array) ? result : [ result ]
        to_write.each do |io|
          AWS::S3::S3Object.store(sha1(io), io, Juggler.config['processed_bucket_name'])
        end
        self.status = 'completed'
      else
        self.status = 'failed'
      end
    end

    def id 
      if @id
        return @id
      else
        Digest::SHA1.hexdigest(@object.value).to_s
      end
    end

    def status=(value)
      Juggler.redis.hset 'juggler.status', self.id, value
    end

    def status
      Juggler.redis.hget 'juggler.status', self.id
    end

    def progress=(value)
      Juggler.redis.hset 'juggler.progress', self.id, value
    end

    def progress
      (Juggler.redis.hget 'juggler.progress', self.id).to_f
    end

    def set_data(key, value)
      Juggler.redis.hset 'juggler.data', self.id, JSON.dump(data.merge(key => value))
    end

    def data
      str = Juggler.redis.hget 'juggler.data', self.id
      str ? JSON.parse(str) : {}
    end

    def sha1(io)
      sha1 = Digest::SHA1.new
			counter = 0
			while (!io.eof)
				buffer = io.readpartial(4096)
				sha1.update(buffer)
			end
      return sha1.hexdigest
    end

    def lock(object)
      parts = File.split(object.path)
      name = "---LOCKED---#{parts.last}"
      object.rename(name)
      Juggler.queue_bucket[name]
    end

    def perform(io)
      Juggler.processor.new(self).run(io)
    end

    def cleanup(object)
      object.delete
    end
  end

  class Processor
    def initialize(job)
      @job = job
    end

    attr_reader :job

    def progress(value)
      job.progress = value
    end

    def set(key, value)
      job.set_data key, value
    end

    def get(key)
      job.data[key]
    end
  end

  class PassthroughProcessor < Processor
    def run(io)
      io
    end
  end

end

Juggler.processor = Juggler::PassthroughProcessor

