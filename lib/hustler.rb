$:.unshift File.join(File.dirname(__FILE__),'..','lib')

require 'rubygems'
require 'bundler'
require 'digest/sha1'
require 'json'

Bundler.require

require 'hustler/cli'

module Hustler

  DEFAULT_OPTIONS =
    {
      'redis_host' => 'localhost',
      'redis_port' => 6379
    }.freeze

  class OptionsHash < Hash
    def initialize(hash)
      self.merge!(hash)
    end

    def method_missing(name, *args)
      self[name.to_s] || self[name]
    end
  end

  class << self

    def redis
      @redis ||= Redis.new(:host => @config['redis_host'], :port => @config['redis_port'])
    end

    def config
      @config 
    end

    def configure(options)
      @config = OptionsHash.new(DEFAULT_OPTIONS.merge(options))
      AWS::S3::Base.establish_connection!(
        :access_key_id     => @config['access_key_id'],
        :secret_access_key => @config['secret_access_key']
      )
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

    def queue_bucket
      AWS::S3::Bucket.find(self.config['queue_bucket_name'])
    end

    def processed_bucket
      AWS::S3::Bucket.find(self.config['processed_bucket_name'])
    end

    def processed_url(id)
      "http://#{self.config['processed_bucket_name']}.s3.amazonaws.com/#{id}"
    end

    def url_for(id)
      options = Hustler.config.expire_urls_in ? { :expires_in => Hustler.config.expire_urls_in } : {}
      AWS::S3::S3Object.url_for(id, self.config.processed_bucket_name, options)
    end

    def queue
      queue_bucket.objects.reject {|o| o.path.include?("---LOCKED---")}
    end

    def store(io, bucket)
      AWS::S3::S3Object.store(sha1(io), io, bucket, :access => Hustler.config.access_policy.intern)
    end

    def enqueue(file)
      store(file, self.config.queue_bucket_name)
    end

    attr_accessor :processor
  end

  class Worker

    def self.run
      new.run
    end

    def initialize
    end

    def run
      Hustler.queue.each do |object|
        Hustler::Job.run(object)
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
          Hustler.store(io, Hustler.config.processed_bucket_name)
        end
        self.status = 'completed'
        processor.on_complete(self)
      else
        self.status = 'failed'
        processor.on_error(self)
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
      Hustler.redis.hset 'hustler.status', self.id, value
    end

    def status
      Hustler.redis.hget 'hustler.status', self.id
    end

    def progress=(value)
      Hustler.redis.hset 'hustler.progress', self.id, value
    end

    def progress
      (Hustler.redis.hget 'hustler.progress', self.id).to_f
    end

    def set_data(key, value)
      Hustler.redis.hset 'hustler.data', self.id, JSON.dump(data.merge(key => value))
    end

    def data
      str = Hustler.redis.hget 'hustler.data', self.id
      str ? JSON.parse(str) : {}
    end

    def sha1(io)
      Hustler.sha1(io)
    end

    def lock(object)
      parts = File.split(object.path)
      name = "---LOCKED---#{parts.last}"
      object.rename(name)
      Hustler.queue_bucket[name]
    end

    def processor
      @processor ||= Hustler.processor.new(self)
    end

    def perform(io)
      processor.run(io)
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
    
    def on_complete(job)
      # default noop
    end

    def on_error(job)
      # default noop
    end
  end

  class PassthroughProcessor < Processor
    def run(io)
      io
    end
  end

end

Hustler.processor      = Hustler::PassthroughProcessor

