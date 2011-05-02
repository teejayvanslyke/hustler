require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe Juggler do

  def mock_s3_object(overrides={})
    mock(AWS::S3::S3Object, {
      :path => '/path/to/file', :rename => nil, :delete => nil, :value => 'val'
    }.merge(overrides))
  end

  before :each do 
    @bucket = mock(AWS::S3::Bucket, :objects => [ ], :[] => nil)
    AWS::S3::Bucket.stub!(:find).
      and_return(@bucket)
    AWS::S3::S3Object.stub!(:store)

    @s3_object = mock_s3_object
    Juggler.queue_bucket.stub!(:objects).and_return([ @s3_object ])
    Juggler.queue_bucket.stub!(:[]).and_return(@s3_object)
  end

  describe Juggler::Worker do

    it "should poll for whether there is a file in the queue" do
      Juggler.queue_bucket.should_receive(:objects)
      Juggler::Worker.run
    end

    context "when there is a file in the queue" do
      it "should process the file" do
        Juggler::Job.should_receive(:run).with(@s3_object)
        Juggler::Worker.run
      end
    end

    context "when all the files in the queue are locked" do
      before :each do
        @locked_s3_object = mock_s3_object(:path => '/path/to/---LOCKED---file')
        Juggler.queue_bucket.stub!(:objects).and_return([ @locked_s3_object ])
      end

      it "shouldn't process the files" do
        Juggler::Job.should_not_receive(:run).with(@locked_s3_object)
        Juggler::Worker.run
      end
    end
  end

  describe Juggler::Job do

    before :each do
      @s3_object = mock_s3_object(:path => "/the_bucket/filename")
      Juggler.queue_bucket.stub!(:[]).and_return(@s3_object)
      class TestProcessor
        def run(io)
          return StringIO.new("FOOBAR")
        end
      end

      Juggler.processor = TestProcessor.new
    end


    context "and the file already has the lock prefix" do
      before :each do
        @s3_object.stub!(:path).and_return("---LOCKED---foobar")
      end

      it "should skip processing" do
        Juggler::Job.run(@s3_object)
      end
    end

    it "should prepend a lock prefix to the object path" do
      @s3_object.should_receive(:rename).with("---LOCKED---filename")
      Juggler::Job.run(@s3_object)
    end

    it "should process the file through the processor" do
      AWS::S3::S3Object.should_receive(:store).
        with(Digest::SHA1.hexdigest("FOOBAR"), anything, Juggler.config['processed_bucket_name'])
      Juggler::Job.run(@s3_object)
    end
  end
end

