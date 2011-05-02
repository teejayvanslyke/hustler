require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

def mock_s3_object(overrides={})
  mock(AWS::S3::S3Object, {
    :path => '/path/to/file', :rename => nil, :delete => nil, :value => 'val'
  }.merge(overrides))
end

describe Juggler::Worker do

  before :each do 
    @bucket = mock(AWS::S3::Bucket, :objects => [ ], :[] => nil)
    AWS::S3::Bucket.stub!(:find).
      and_return(@bucket)
    AWS::S3::S3Object.stub!(:store)
  end

  it "should poll for whether there is a file in the queue" do
    Juggler.queue_bucket.should_receive(:objects)
    Juggler::Worker.run
  end

  context "when there is a file in the queue" do
    before :each do
      @s3_object = mock_s3_object
      Juggler.queue_bucket.stub!(:objects).and_return([ @s3_object ])
      Juggler.queue_bucket.stub!(:[]).and_return(@s3_object)
    end

    it "should process the file" do
      Juggler::Job.should_receive(:run).with(@s3_object)
      Juggler::Worker.run
    end
  end

end

describe Juggler::Job do

  before :each do
    @bucket = mock(AWS::S3::Bucket, :objects => [ ], :[] => nil)
    @s3_object = mock_s3_object(:path => "/the_bucket/filename")
    AWS::S3::Bucket.stub!(:find).
      and_return(@bucket)
    AWS::S3::S3Object.stub!(:store)
    Juggler.queue_bucket.stub!(:[]).and_return(@s3_object)
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

  end
end

