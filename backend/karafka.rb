# frozen_string_literal: true

ENV["KARAFKA_ENV"] ||= "development"

require_relative "config/boot"

class App < Karafka::App
  setup do |config|
    config.kafka = {
      :"bootstrap.servers" => ENV.fetch("KAFKA_BROKERS", "kafka:9092"),
      :"client.id" => "pix-karafka",
      :"allow.auto.create.topics" => true
    }
    config.client_id = "pix-karafka"
    config.concurrency = 1
    config.logger = Logger.new($stdout, level: Logger::INFO)
    config.initial_offset = "latest"
    config.max_wait_time = 1_000 # 1 second
    config.shutdown_timeout = 25_000 # 25 seconds
    config.max_messages = 100
    config.consumer_persistence = false
  end

  # https://github.com/karafka/karafka/blob/master/lib/karafka/instrumentation/notifications.rb
  Karafka.monitor.subscribe(Karafka::Instrumentation::LoggerListener.new)

  Karafka.monitor.subscribe "error.occurred" do |event|
    error, payload = event[:error], event.payload
    # puts "Kafka Error #{error.class}: #{error.message}"
    Karafka.logger.error("Kafka Error #{error.class}: #{error.message}")
  end

  routes.draw do
    consumer_group :pix_requests_group do
      topic :pix_requests do
        consumer PixRequestConsumer
      end
    end

    consumer_group :pix_responses_group do
      topic :pix_responses do
        consumer PixResponseBridgeConsumer
      end
    end
  end
end
