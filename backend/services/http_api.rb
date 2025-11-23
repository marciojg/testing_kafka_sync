# frozen_string_literal: true

require_relative "../karafka"
require "sinatra/base"
require "json"
require "securerandom"
require 'logger'

class PixApi < Sinatra::Base
  configure do
    set :bind, "0.0.0.0"
    set :port, ENV.fetch("API_PORT", "4000").to_i
    set :server, :webrick
    enable :logging
    set :logger, Logger.new($stdout)
  end

  helpers do
    def logger
      settings.logger
    end
  end

  before do
    content_type :json
  end

  post "/pix" do
    payload = parse_payload
    message = build_message(payload)

    logger.info("[api] requesta PIX recebida: #{message.fetch('correlationId')}")

    Karafka::App.producer.produce_async(
      topic: "pix_requests",
      payload: JSON.dump(message)
    )

    logger.info("[api] publicado PIX request: #{message.fetch('correlationId')}")

    status 202
    {
      ok: true,
      clientId: message.fetch("clientId"),
      requestId: message.fetch("correlationId")
    }.to_json
  rescue JSON::ParserError
    halt 400, { ok: false, error: "invalid JSON" }.to_json
  rescue StandardError => e
    logger.error("[api] erro ao publicar PIX: #{e.message}")
    halt 500, { ok: false, error: "internal error" }.to_json
  end

  get "/health" do
    { ok: true }.to_json
  end

  helpers do
    def parse_payload
      body = request.body.read
      halt 400, { ok: false, error: "empty body" }.to_json if body.strip.empty?
      JSON.parse(body)
    end

    def build_message(payload)
      client_id = payload["clientId"] || SecureRandom.uuid
      correlation_id = payload["correlationId"] || SecureRandom.uuid
      {
        "payer" => payload.fetch("payer"),
        "payee" => payload.fetch("payee"),
        "amount" => payload.fetch("amount"),
        "description" => payload["description"],
        "clientId" => client_id,
        "correlationId" => correlation_id,
        "responseSubject" => response_subject(client_id, correlation_id),
        "status" => "queued",
        "createdAt" => Time.now.utc.iso8601
      }
    rescue KeyError => e
      halt 400, { ok: false, error: "missing field #{e.key}" }.to_json
    end

    def response_subject(client_id, correlation_id)
      "pix.responses.#{client_id}.#{correlation_id}"
    end
  end
end

# PixApi.run! if $PROGRAM_NAME == __FILE__
