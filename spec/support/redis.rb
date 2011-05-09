class Redis

	def initialize(options={})
		@hashes = {}
	end

	def hset(name, key, value)
		@hashes[name] ||= {}
		@hashes[name][key] = value
	end

	def hget(name, key)
		@hashes[name] ||= {}
		@hashes[name][key]
	end

end

