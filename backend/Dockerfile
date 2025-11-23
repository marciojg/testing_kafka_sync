# Ruby + Karafka + NATS bridge
FROM ruby:3.2-alpine

RUN apk add --no-cache build-base libstdc++ tzdata

WORKDIR /app
COPY Gemfile Gemfile.lock* ./
RUN bundle install

COPY . .

ENV KAFKA_BROKERS=kafka:9092 \
    NATS_URL=nats://nats:4222 \
    API_PORT=4000 \
    RACK_ENV=production

# Default command serves the HTTP API (consumer overrides via docker-compose)
CMD ["bundle", "exec", "ruby", "services/http_api.rb"]
