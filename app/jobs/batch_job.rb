class BatchJob < ApplicationJob
  queue_as :default

  # NOTE observability etc omitted for clarity

  DEFAULT_BATCH_SIZE = 100

  def perform(*args, offset_id: 0)
    batch = fetch_batch(offset_id:)
    batch.each do |item|
      process_item(item)
      offset_id = item_id(item)
    end

    # If this wasn't a full batch, we reached the end.
    if batch.size < batch_size
      p "Partial batch; job complete. batch.size:#{batch.size} batch_size:#{batch_size}"
      return
    end

    # Otherwise there is more to do; schedule a subsequent job.
    p "Batch complete; scheduling next job. offset_id:#{offset_id}"
    self.class.perform_later(*args, offset_id:)
  end

  protected

  # The number of items to fetch/process in a batch. Default 100.
  # Override if your job implementation needs more/less items, 
  # for example if your item processing is faster/slower than usual.
  def batch_size
    DEFAULT_BATCH_SIZE
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
end
