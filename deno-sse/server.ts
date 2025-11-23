// Simple SSE gateway that bridges HTTP calls to NATS JetStream and relays
// Kafka results (bridged back to JetStream) to connected clients.
import { serve } from "https://deno.land/std@0.223.0/http/server.ts";
import { serveDir } from "https://deno.land/std@0.223.0/http/file_server.ts";
import {
  JSONCodec,
  connect,
  consumerOpts,
} from "https://deno.land/x/nats/src/mod.ts";

type PixRequest = {
  payer: string;
  payee: string;
  amount: number;
  description?: string;
};

type PixMessage = PixRequest & {
  correlationId: string;
  clientId: string;
  responseSubject: string;
  createdAt: string;
  status?: "queued" | "processing" | "done" | "failed";
  error?: string;
};

const NATS_URL = Deno.env.get("NATS_URL") ?? "nats://127.0.0.1:4222";
const BACKEND_URL = Deno.env.get("BACKEND_URL") ?? "http://127.0.0.1:4000";
const PORT = Number(Deno.env.get("PORT") ?? 8000);
const WEB_ROOT = new URL("../web", import.meta.url).pathname;
const jc = JSONCodec<PixMessage>();
const clients = new Map<string, ReadableStreamDefaultController<Uint8Array>>();

const nc = await connect({ servers: NATS_URL });
const js = nc.jetstream();
const jsm = await nc.jetstreamManager();

await ensureStream();
await startResponseSubscription();

console.log(
  `[gateway] listening on http://localhost:${PORT} (NATS: ${NATS_URL})`,
);

serve(handle, { port: PORT });

async function handle(req: Request): Promise<Response> {
  const url = new URL(req.url);

  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: corsHeaders() });
  }

  if (req.method === "GET" && url.pathname === "/events") {
    const clientId = url.searchParams.get("clientId") ?? crypto.randomUUID();
    return openSse(clientId);
  }

  if (req.method === "POST" && url.pathname === "/pix") {
    return publishPix(req);
  }

  // Serve the static React POC.
  return serveDir(req, { fsRoot: WEB_ROOT, urlRoot: "" });
}

async function publishPix(req: Request): Promise<Response> {
  try {
    const body = (await req.json()) as PixRequest & { clientId?: string };
    const clientId = body.clientId ?? crypto.randomUUID();
    const payload = { ...body, clientId };
    const backendRes = await fetch(`${BACKEND_URL}/pix`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await backendRes.json().catch(() => ({}));
    console.log("[DEBUG] backend data:", data);

    return json({ ok: true, clientId, requestId: data["requestId"] }, 202);
  } catch (error) {
    console.error("[gateway] publishPix error", error);
    return json({ ok: false, error: String(error) }, 400);
  }
}

function openSse(clientId: string): Response {
  let timer: number | undefined;
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      clients.set(clientId, controller);
      controller.enqueue(encodeSse({ type: "connected", clientId }));
      console.log(`[gateway] SSE conectado: ${clientId}`);

      timer = setTimeout(() => {
        controller.enqueue(
          encodeSse({
            type: "error",
            error: "Conexão SSE encerrada após 30s",
          }),
        );
        controller.error(new Error("SSE encerrada após 30s"));
        clients.delete(clientId);
      }, 30_000);

      controller.enqueue(
        encodeSse({ type: "info", data: "SSE ficará ativo por 30s" }),
      );

      // No controller.onCancel in standard ReadableStream; cleanup is handled in cancel().
    },
    cancel() {
      if (timer !== undefined) {
        clearTimeout(timer);
      }
      clients.delete(clientId);
    },
  });

  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      "connection": "keep-alive",
      ...corsHeaders(),
    },
  });
}

async function startResponseSubscription() {
  const opts = consumerOpts();
  opts.durable("pix-responses");
  opts.manualAck();
  opts.ackExplicit();
  opts.deliverAll();
  opts.bindStream("PIX");
  opts.filterSubject("pix.responses.>");
  opts.deliverTo(`pix-deliver-${crypto.randomUUID()}`);

  const sub = await js.subscribe("pix.responses.>", opts);

  (async () => {
    for await (const msg of sub) {
      const payload = jc.decode(msg.data);
      sendToClient(payload.clientId, {
        type: "pix:update",
        data: payload,
      });
      await msg.ack();
    }
  })();
}

function sendToClient(
  clientId: string,
  event: Record<string, unknown>,
): void {
  const controller = clients.get(clientId);
  if (!controller) return;
  controller.enqueue(encodeSse(event));
}

async function ensureStream() {
  try {
    await jsm.streams.info("PIX");
  } catch {
    await jsm.streams.add({
      name: "PIX",
      subjects: ["pix.requests", "pix.responses.>"],
      retention: "limits",
      max_msgs_per_subject: 10_000,
    });
  }
}

function encodeSse(event: Record<string, unknown>): Uint8Array {
  return new TextEncoder().encode(`data: ${JSON.stringify(event)}\n\n`);
}

function json(body: Record<string, unknown>, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json",
      ...corsHeaders(),
    },
  });
}

function corsHeaders(): Record<string, string> {
  return {
    "access-control-allow-origin": "*",
    "access-control-allow-headers": "content-type",
    "access-control-allow-methods": "GET,POST,OPTIONS",
  };
}
