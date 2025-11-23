# frozen_string_literal: true

require "nats/io/client"
require_relative "../../config/boot"

class PixResponseBridgeConsumer < Karafka::BaseConsumer
  def consume
    messages.each do |message|
      raw_payload = message.payload
      Karafka.logger.info "[pix] PixRequestConsumer recebeu mensagem: #{raw_payload.inspect}"
      payload = begin
        case raw_payload.class.name
        when "String"
          JSON.parse(raw_payload)
        when "Hash"
          raw_payload
        else
          raise "Tipo de payload inesperado: #{raw_payload.class.name}"
        end
      end

      publish_to_nats(payload)
    end
  end

  private

  def publish_to_nats(payload)
    subject = payload.fetch("responseSubject")
    jetstream.publish(subject, JSON.dump(payload))
    Karafka.logger.info("[pix] published to NATS JS subject=#{subject}")
    Karafka.logger.info("[pix] resposta enviada #{payload['correlationId']}")
  rescue NATS::JetStream::Error => e
    Karafka.logger.error("[pix] falha ao publicar no NATS: #{e.message}")
    reset_jetstream!
  rescue StandardError => e
    Karafka.logger.error("[pix] falha inesperada ao publicar no NATS: #{e.class} #{e.message}")
    reset_jetstream!
  end

  def nats_connection
    @@nats_connection ||= begin
      conn = NATS::IO::Client.new
      conn.connect(servers: [ENV.fetch("NATS_URL", "nats://127.0.0.1:4222")])
      at_exit { conn.close }
      conn
    end
  end

  def jetstream
    @@jetstream ||= begin
      js = nats_connection.jetstream
      ensure_pix_stream(js)
      js
    end
  end

  def ensure_pix_stream(js)
    info = js.stream_info("PIX")
    subjects = Array(info.dig(:config, :subjects))
    desired_subjects = ["pix.requests", "pix.responses.>"]
    return if (desired_subjects - subjects).empty?

    js.update_stream(name: "PIX", subjects: desired_subjects)
    Karafka.logger.info("[pix] stream PIX atualizado com novas subjects")
  rescue NATS::JetStream::Error => e
    if e.message =~ /not found/i
      js.add_stream(name: "PIX", subjects: ["pix.requests", "pix.responses.>"])
      Karafka.logger.info("[pix] stream PIX criado no JetStream")
    else
      Karafka.logger.warn("[pix] erro ao verificar stream PIX: #{e.message}")
    end
  end

  def reset_jetstream!
    @@jetstream = nil
    @@nats_connection = nil
    jetstream
  rescue StandardError => e
    Karafka.logger.error("[pix] falha ao reinicializar JetStream: #{e.message}")
  end
end
