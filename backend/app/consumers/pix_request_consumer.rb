# frozen_string_literal: true

require_relative "../../config/boot"

class PixRequestConsumer < Karafka::BaseConsumer
  def consume
    Karafka.logger.info "[pix] PixRequestConsumer"
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

      # Simula passo de negócio e envia resposta para o tópico de saída.
      sleep(1)
      Karafka.logger.info "[pix] PixRequestConsumer processado #{payload['correlationId']} com sucesso"

      response = payload.merge(
        "status" => "done",
        "processed_at" => Time.now.utc.iso8601
      )

      begin
        report = Karafka.producer.produce_sync(
          topic: "pix_responses",
          payload: JSON.dump(response)
        )

        if report.error.nil?
          Karafka.logger.info("[pix] resposta publicada correlationId=#{response['correlationId']} topic=pix_responses partition=#{report.partition} offset=#{report.offset}")
        else
          Karafka.logger.error("[pix] falha ao publicar correlationId=#{response['correlationId']} error=#{report.error}")
        end

        Karafka.logger.info "[pix] PixRequestConsumer resposta publicada para #{payload['correlationId']}"
      rescue StandardError => e
        Karafka.logger.error("[pix] exceção ao publicar correlationId=#{response['correlationId']} error=#{e.class}: #{e.message}")
        raise
      end

      mark_as_consumed(message)
    rescue StandardError => e
      Karafka.logger.error("[pix] exceção ao processar mensagem error=#{e.class}: #{e.message}")
      nil # force oack
    end
  end
end
