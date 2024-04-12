# frozen_string_literal: true

class SamplePlainJob
  include Sidekiq::Worker

  def perform(*_args)
    "My job is simple"
  end
end

class SampleThrottledJob
  include Sidekiq::Worker

  sidekiq_options queue: :whatever,
                  fair_tenant_queues: [
                    { queue: :whatever_semislow, threshold: 1, per: 1.day },
                    { queue: :whatever_supaslow, threshold: 1, per: 1.hour }
                  ]
end

class SampleThrottledActiveJob < ActiveJob::Base
  self.queue_adapter = :sidekiq

  queue_as :whatever

  if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("6.0.1")
    include Sidekiq::Worker::Options

    sidekiq_options fair_tenant_queues: [
      { queue: :whatever_semislow, threshold: 1, per: 1.day },
      { queue: :whatever_supaslow, threshold: 1, per: 1.hour }
    ]
  end

  def self.fair_tenant(arg1 = "foo", *)
    arg1
  end

  def perform(_arg1, *)
    "I'm doing my job"
  end
end

class SampleThrottledSlottedJob
  include Sidekiq::Worker

  sidekiq_options queue: :whatever,
                  slotted_queues: [
                    :whatever_a,
                    :whatever_b,
                    :whatever_c
                  ],
                  fair_tenant_queues: [
                    {
                      queue: :whatever_a_semislow,
                      slots: [:whatever_a],
                      threshold: 1,
                      per: 1.day
                    },
                    {
                      queue: :whatever_b_c_semislow,
                      slots: [:whatever_b, :whatever_c],
                      threshold: 1,
                      per: 1.day
                    },
                    {
                      queue: :whatever_a_supaslow,
                      slots: [:whatever_a],
                      threshold: 1,
                      per: 1.hour
                    },
                    {
                      queue: :whatever_b_supaslow,
                      slots: [:whatever_b],
                      threshold: 1,
                      per: 1.hour
                    },
                    {
                      queue: :whatever_c_supaslow,
                      slots: [:whatever_c],
                      threshold: 1,
                      per: 1.hour
                    }
                  ]
end

class SampleThrottledSlottedActiveJob < ActiveJob::Base
  self.queue_adapter = :sidekiq

  queue_as :whatever

  if Gem::Version.new(Sidekiq::VERSION) >= Gem::Version.new("6.0.1")
    include Sidekiq::Worker::Options

    sidekiq_options slotted_queues: [
      :whatever_a,
      :whatever_b,
      :whatever_c
    ],
    fair_tenant_queues: [
      {
        queue: :whatever_a_semislow,
        slots: [:whatever_a],
        threshold: 1,
        per: 1.day
      },
      {
        queue: :whatever_b_c_semislow,
        slots: [:whatever_b, :whatever_c],
        threshold: 1,
        per: 1.day
      },
      {
        queue: :whatever_a_supaslow,
        slots: [:whatever_a],
        threshold: 1,
        per: 1.hour
      },
      {
        queue: :whatever_b_supaslow,
        slots: [:whatever_b],
        threshold: 1,
        per: 1.hour
      },
      {
        queue: :whatever_c_supaslow,
        slots: [:whatever_c],
        threshold: 1,
        per: 1.hour
      }
    ]
  end

  def self.fair_tenant(arg1 = "foo", *)
    arg1
  end

  def perform(_arg1, *)
    "I'm doing my job"
  end
end
