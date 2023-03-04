class TimedJob < ApplicationJob
  queue_as :default

  # NOTE observability etc omitted for clarity

  DEFAULT_BATCH_SIZE = 100
  DEFAULT_MAX_TIME = 1.minute

  def perform(*args, offset_id: 0)
    SafeTimer.timeout(max_time) do |timer|
      loop do
        batch = fetch_batch(offset_id:)
        batch.each do |item|
          process_item(item)
          offset_id = item_id(item)
          break if timer.expired?
        end

        # If the timer has expired, stop here and schedule 
        # the next job to pick up where we left off.
        if timer.expired?
          p "Times up; scheduling next job. offset_id:#{offset_id}"
          self.class.perform_later(*args, offset_id:)
          break
        end

        # If this wasn't a full batch, we reached the end.
        if batch.size < batch_size
          p "Partial batch; job complete. batch.size:#{batch.size} batch_size:#{batch_size}"
          break
        end
        
        p "Batch complete; fetching next batch. offset_id:#{offset_id}"
      end
    end
  end

  protected

  # The number of items to fetch/process in a batch. Default 100.
  # Override if your job implementation needs more/less items, 
  # for example if your item processing is faster/slower than usual.
  def batch_size
    DEFAULT_BATCH_SIZE
  end

  # The max processing time before the job is requeued. Default 1 minute.
  # Override to process more/less items per job to balance 
  # individual job time vs total processing time.
  def max_time
    DEFAULT_MAX_TIME
  end

  # Return the primary id for the given item. Default `item.id`
  # Override if your item does not have an `#id` field.
  def item_id(item)
    item.id
  end

  # Abstract. Fetch a batch of items for processing.
  # Use the equivalent of
  #   where id > offset_id
  #   order id
  #   limit batch_size
  def fetch_batch(offset_id:)
    raise NotImplementedError.new
  end

  # Abstract. Process a single item.
  def process_item(item)
    raise NotImplementedError.new
  end
  
  class SafeTimer
    def self.timeout(max, &block)
      yield TimerImpl.new(max)
    end

    TimerImpl = Struct.new(:max) do
      def initialize(max)
        @expiry = Time.now + max
      end

      def expired?
        Time.now > @expiry
      end
    end
  end
end
