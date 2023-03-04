require "test_helper"

class BatchJobTest < ActiveJob::TestCase
  # test "the truth" do
  #   assert true
  # end

  test "batch requeuing works" do
    perform_enqueued_jobs(only: TestJob) do
      TestJob.perform_later
    end
    assert_performed_jobs(3, only: TestJob)
  end

  class TestJob < BatchJob

    TestItem = Struct.new(:id, :name, keyword_init: true)
    DUMMY_ITEMS = (1..123).map do |i|
      TestItem.new(id: i, name: "item-#{i}")
    end

    def batch_size
      50
    end

    def fetch_batch(offset_id:)
      DUMMY_ITEMS
        .select { |x| x.id > offset_id }
        .sort_by(&:id)
        .take(batch_size)
    end

    def process_item(item)
      p item.name
    end
  end
end
