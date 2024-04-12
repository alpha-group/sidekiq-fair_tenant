# frozen_string_literal: true

RSpec.describe Sidekiq::FairTenant do
  context "when throttling is not set up" do
    it "enqueues job to default job" do
      expect { SamplePlainJob.perform_async }.to enqueue_sidekiq_job.on("default")
    end
  end

  context "when throttling is set up" do
    subject(:enqueuer) { ->(**args) { SampleThrottledJob.set(**args).perform_async } }

    it "re-routes jobs above threshold for a single tenant" do
      # 1st job should be enqueued to the default queue
      expect { enqueuer.call(fair_tenant: :foo) }.to enqueue_sidekiq_job.on("whatever")

      # 2nd job should be enqueued to the slow queue as it exceeds threshold of 1
      expect { enqueuer.call(fair_tenant: :foo) }.to enqueue_sidekiq_job.on("whatever_supaslow")

      # However, 3rd job should be enqueued to the default queue as it is for a different tenant
      expect { enqueuer.call(fair_tenant: :bar) }.to enqueue_sidekiq_job.on("whatever")

      # And 4th job should be enqueued to the less slow queue as its super slow queue window has passed
      travel 1.hour + 1.second
      expect { enqueuer.call(fair_tenant: :foo) }.to enqueue_sidekiq_job.on("whatever_semislow")

      # After all time windows have passed, 5th job should be enqueued to the default queue
      travel 1.day + 1.second
      expect { enqueuer.call(fair_tenant: :foo) }.to enqueue_sidekiq_job.on("whatever")
    end

    context "when slotting is set up" do
      subject(:enqueuer) { ->(**args) { SampleThrottledSlottedJob.set(**args).perform_async } }

      it "re-routes jobs above threshold for a single tenant" do
        # 1st job should be enqueued to the specified slotted queue
        expect { enqueuer.call(queue: :whatever_a, fair_tenant: :foo) }.to enqueue_sidekiq_job.on("whatever_a")

        # 2nd job should be enqueued to the slotted slow queue as it exceeds threshold of 1
        expect { enqueuer.call(queue: :whatever_a, fair_tenant: :foo) }.to enqueue_sidekiq_job.on("whatever_a_supaslow")

        # However, 3rd job should be enqueued to the specified slotted queue as it is for a different tenant
        expect { enqueuer.call(queue: :whatever_a, fair_tenant: :bar) }.to enqueue_sidekiq_job.on("whatever_a")

        # And 4th job should be enqueued to the slotted less slow queue as its super slow queue window has passed
        travel 1.hour + 1.second
        expect { enqueuer.call(queue: :whatever_a, fair_tenant: :foo) }.to enqueue_sidekiq_job.on("whatever_a_semislow")

        # After all time windows have passed, 5th job should be enqueued to the specified slotted queue
        travel 1.day + 1.second
        expect { enqueuer.call(queue: :whatever_a, fair_tenant: :foo) }.to enqueue_sidekiq_job.on("whatever_a")
      end
    end
  end

  context "when another queue has been specified for a job at enqueue time" do
    context "when slotting is not set up" do
      subject(:enqueuer) { -> { SampleThrottledJob.set(queue: :another, fair_tenant: :foo).perform_async } }

      it "doesn't re-route jobs" do
        # 1st job should be enqueued to the specified queue
        expect { enqueuer.call }.to enqueue_sidekiq_job.on("another")

        # 2nd job should also be enqueued to the specified queue even if it exceeds threshold of 1
        expect { enqueuer.call }.to enqueue_sidekiq_job.on("another")
      end
    end

    context "when slotting is set up" do
      context "when the specified queue is a slotted queue" do
        subject(:enqueuer) { ->(**args) { SampleThrottledSlottedJob.set(**args).perform_async } }

        it "re-routes jobs above threshold for a single tenant" do
          # 1st job should be enqueued to the specified slotted queue
          expect { enqueuer.call(queue: :whatever_c, fair_tenant: :foo) }.to enqueue_sidekiq_job.on("whatever_c")

          # 2nd job should be enqueued to the slotted slow queue as it exceeds threshold of 1
          expect { enqueuer.call(queue: :whatever_c, fair_tenant: :foo) }.to enqueue_sidekiq_job.on("whatever_c_supaslow")
        end
      end

      context "when the specified queue is not a slotted queue" do
        subject(:enqueuer) { ->(**args) { SampleThrottledSlottedJob.set(**args).perform_async } }

        it "doesn't re-route jobs" do
          # 1st job should be enqueued to the specified queue
          expect { enqueuer.call(queue: :another, fair_tenant: :foo) }.to enqueue_sidekiq_job.on("another")

          # 2nd job should also be enqueued to the specified queue even if it exceeds threshold of 1
          expect { enqueuer.call(queue: :another, fair_tenant: :foo) }.to enqueue_sidekiq_job.on("another")
        end
      end
    end
  end

  context "using ActiveJob" do
    subject(:enqueuer) { ->(*args) { SampleThrottledActiveJob.perform_later(*args) } }

    it "re-routes jobs above threshold for a single tenant" do
      skip("ActiveJob supported only in Sidekiq 6+") if Gem::Version.new(Sidekiq::VERSION) < Gem::Version.new("6.0.1")

      # 1st job should be enqueued to the default queue
      expect { enqueuer.call }.to enqueue_sidekiq_job.on("whatever")

      # 2nd job should be enqueued to the slow queue as it exceeds threshold of 1
      expect { enqueuer.call }.to enqueue_sidekiq_job.on("whatever_supaslow")

      # However, 3rd job should be enqueued to the default queue as it is for a different tenant
      expect { enqueuer.call("bar") }.to enqueue_sidekiq_job.on("whatever")

      # And 4th job should be enqueued to the less slow queue as it super slow queue window has passed
      travel 1.hour + 1.second
      expect { enqueuer.call }.to enqueue_sidekiq_job.on("whatever_semislow")

      # After all time windows have passed, 5th job should be enqueued to the default queue
      travel 1.day + 1.second
      expect { enqueuer.call }.to enqueue_sidekiq_job.on("whatever")
    end

    context "when slotting is set up" do
      subject(:enqueuer) { ->(fair_tenant: "foo", **args) { SampleThrottledSlottedActiveJob.set(**args).perform_later(fair_tenant) } }

      it "re-routes jobs above threshold for a single tenant" do
        skip("ActiveJob supported only in Sidekiq 6+") if Gem::Version.new(Sidekiq::VERSION) < Gem::Version.new("6.0.1")

        # 1st job should be enqueued to the specified slotted queue
        expect { enqueuer.call(queue: :whatever_a, fair_tenant: :foo) }.to enqueue_sidekiq_job.on("whatever_a")

        # 2nd job should be enqueued to the slotted slow queue as it exceeds threshold of 1
        expect { enqueuer.call(queue: :whatever_a, fair_tenant: :foo) }.to enqueue_sidekiq_job.on("whatever_a_supaslow")

        # However, 3rd job should be enqueued to the specified slotted queue as it is for a different tenant
        expect { enqueuer.call(queue: :whatever_a, fair_tenant: :bar) }.to enqueue_sidekiq_job.on("whatever_a")

        # And 4th job should be enqueued to the slotted less slow queue as its super slow queue window has passed
        travel 1.hour + 1.second
        expect { enqueuer.call(queue: :whatever_a, fair_tenant: :foo) }.to enqueue_sidekiq_job.on("whatever_a_semislow")

        # After all time windows have passed, 5th job should be enqueued to the specified slotted queue
        travel 1.day + 1.second
        expect { enqueuer.call(queue: :whatever_a, fair_tenant: :foo) }.to enqueue_sidekiq_job.on("whatever_a")
      end
    end
  end

  context "when another queue has been specified for an ActiveJob at enqueue time" do
    context "when slotting is not set up" do
      subject(:enqueuer) { -> { SampleThrottledActiveJob.set(queue: :another).perform_later } }

      it "doesn't re-route jobs" do
        skip("ActiveJob supported only in Sidekiq 6+") if Gem::Version.new(Sidekiq::VERSION) < Gem::Version.new("6.0.1")

        # 1st job should be enqueued to the default queue
        expect { enqueuer.call }.to enqueue_sidekiq_job.on("another")

        # 2nd job should also be enqueued to the set up queue even if it exceeds threshold of 1
        expect { enqueuer.call }.to enqueue_sidekiq_job.on("another")
      end
    end

    context "when slotting is set up" do
      context "when the specified queue is a slotted queue" do
        subject(:enqueuer) { ->(fair_tenant: "foo", **args) { SampleThrottledSlottedActiveJob.set(**args).perform_later(fair_tenant) } }

        it "re-routes jobs above threshold for a single tenant" do
          skip("ActiveJob supported only in Sidekiq 6+") if Gem::Version.new(Sidekiq::VERSION) < Gem::Version.new("6.0.1")

           # 1st job should be enqueued to the specified slotted queue
          expect { enqueuer.call(queue: :whatever_b) }.to enqueue_sidekiq_job.on("whatever_b")

          # 2nd job should be enqueued to the slotted slow queue as it exceeds threshold of 1
          expect { enqueuer.call(queue: :whatever_b) }.to enqueue_sidekiq_job.on("whatever_b_supaslow")

          # And 4th job should be enqueued to the slotted less slow queue as its super slow queue window has passed
          travel 1.hour + 1.second
          expect { enqueuer.call(queue: :whatever_b) }.to enqueue_sidekiq_job.on("whatever_b_c_semislow")
        end
      end

      context "when the specified queue is not a slotted queue" do
        subject(:enqueuer) { ->(fair_tenant: "foo", **args) { SampleThrottledSlottedActiveJob.set(**args).perform_later(fair_tenant) } }

        it "doesn't re-route jobs" do
          skip("ActiveJob supported only in Sidekiq 6+") if Gem::Version.new(Sidekiq::VERSION) < Gem::Version.new("6.0.1")

          # 1st job should be enqueued to the specified queue
          expect { enqueuer.call(queue: :another) }.to enqueue_sidekiq_job.on("another")

          # 2nd job should also be enqueued to the specified queue even if it exceeds threshold of 1
          expect { enqueuer.call(queue: :another) }.to enqueue_sidekiq_job.on("another")
        end
      end
    end
  end
end
