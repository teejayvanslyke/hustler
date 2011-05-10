require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

describe Juggler do

  before :all do
    Juggler.configure(
      'access_key_id'         => '0XZN91V3MBE44A05R2R2',
      'secret_access_key'     => 'WKVJffwxdw+JNzuRTYfj0SmCMgemiFGnIc+KEYRB',
      'queue_bucket_name'     => "queue.hustler.development",
      'processed_bucket_name' => "processed.hustler.development"
    )
  end

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

  describe "when enqueuing a file for processing" do
    it "should upload the file to S3" do
      file = File.open(File.dirname(__FILE__) + '/fixtures/example.jpg')
      AWS::S3::S3Object.should_receive(:store).with(anything, file, Juggler.config['queue_bucket_name'])
      Juggler.enqueue file
    end
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

      $dummy = nil

      class TestProcessor < Juggler::Processor

        def run(io)
          progress 0.5
          set 'test123', { 'key' => 'value' }
          return StringIO.new("FOOBAR")
        end

        def on_complete(job)
          $dummy = "completed"
        end

        def on_error(job)
          $dummy = "failed"
        end
      end

      Juggler.processor = TestProcessor
    end

    it "should have set the data object according to the #put call in the processor" do
      job = Juggler::Job.run(@s3_object)
      job.data['test123'].should == { 'key' => 'value' }
    end

    it "should reflect the appropriate progress status" do
      job = Juggler::Job.run(@s3_object)
      job.progress.should == 0.5
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

    it "should have an initial status of 'queued'" do
      job = Juggler::Job.new(@s3_object)
      job.status.should == 'queued'
    end

    it "should mark the job as 'completed' when completed successfully" do
      job = Juggler::Job.run(@s3_object)
      job.status.should == 'completed'
    end

    it "should run the complete callback when completed successfully" do
      job = Juggler::Job.run(@s3_object)
      $dummy.should == 'completed'
    end

    context "when the job fails" do
      before :each do
        @job = Juggler::Job.new(@s3_object)
        @job.stub!(:perform).and_return(false)
      end

      it "should mark the job as 'failed'" do
        @job.run
        @job.status.should == 'failed'
      end

      it "should run the failure callback" do
        @job.run
        $dummy.should == 'failed'
      end
    end

    context "when using a processor with multiple output streams" do
      before :each do 
        class MultipleOutputProcessor < Juggler::Processor
          def run(io)
            return [ StringIO.new("FOO"), StringIO.new("BAR") ]
          end
        end

        Juggler.processor = MultipleOutputProcessor
      end

      it "should store each as a separate object" do
        %w(FOO BAR).each do |data|
          AWS::S3::S3Object.should_receive(:store).
            with(Digest::SHA1.hexdigest(data), anything, Juggler.config['processed_bucket_name'])
        end

        Juggler::Job.run(@s3_object)
      end
    end

    context "when retrieving an existing job" do
      before :each do 
        @job = Juggler::Job.new(@s3_object)
        @job.run
      end

      it "should return the job instance" do
        job = Juggler::Job.find(@job.id)
        job.should be_a(Juggler::Job)
      end

      it "should retain its status" do
        job = Juggler::Job.find(@job.id)
        job.status.should == 'queued'
      end
    end

  end

end

