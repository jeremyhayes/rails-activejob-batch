require "test_helper"

class TimedJobTest < ActiveJob::TestCase
  # test "the truth" do
  #   assert true
  # end

  test "timeout requeuing works" do
    perform_enqueued_jobs(only: TestJob) do
      TestJob.perform_later
    end
    assert_performed_jobs(3, only: TestJob)
  end

  class TestJob < TimedJob

    TestItem = Struct.new(:id, :name, keyword_init: true)
    DUMMY_ITEMS = (1..123).map do |i|
      TestItem.new(id: i, name: "item-#{i}")
    end

    def max_time
      5.second
    end

    def batch_size
      20
    end

    def fetch_batch(offset_id:)
      DUMMY_ITEMS
        .select { |x| x.id > offset_id }
        .sort_by(&:id)
        .take(batch_size)
    end

    def process_item(item)
      p item.name
      sleep 0.1.seconds
    end
  end
end
