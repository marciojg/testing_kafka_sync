# Deno HTTP + SSE gateway serving the web POC
FROM denoland/deno:1.42.0

WORKDIR /app/deno-sse
COPY deno-sse/deno.json deno-sse/server.ts ./
COPY web /app/web

ENV NATS_URL=nats://nats:4222 \
    BACKEND_URL=http://backend:4000 \
    PORT=8000

EXPOSE 8000

CMD ["run", "--allow-env", "--allow-net", "--allow-read", "server.ts"]
