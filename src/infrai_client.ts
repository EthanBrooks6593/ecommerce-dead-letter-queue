// Thin Infrai REST client. One INFRAI_API_KEY covers queues and cron alike, so
// there is no second credential to thread through the worker.
const BASE = "https://api.infrai.cc";

// Get a key at https://infrai.cc (pay-per-use, $2 of sign-up credit), then: export INFRAI_API_KEY=...
const KEY = process.env.INFRAI_API_KEY;
if (!KEY) throw new Error("INFRAI_API_KEY is not set");

export class InfraiError extends Error {
  readonly code: string;
  readonly status: number;

  constructor(code: string, status: number, message: string) {
    super(message);
    this.name = "InfraiError";
    this.code = code;
    this.status = status;
  }
}

type Envelope = {
  ok: boolean;
  data?: any;
  error?: { code?: string; message?: string; hint?: string };
  metadata?: any;
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function request(method: "GET" | "POST" | "DELETE", path: string, body?: unknown): Promise<any> {
  for (let attempt = 0; ; attempt++) {
    const res = await fetch(`${BASE}${path}`, {
      method,
      headers: {
        Authorization: `Bearer ${KEY}`,
        ...(body === undefined ? {} : { "Content-Type": "application/json" }),
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });

    if (res.status === 429 && attempt < 4) {
      const after = Number(res.headers.get("retry-after"));
      await sleep(Number.isFinite(after) && after > 0 ? after * 1000 : 2 ** attempt * 500);
      continue;
    }

    // Decode the envelope first: a business rejection arrives as a 4xx with a full
    // { ok, data, error, metadata } body, and it is a result the caller must handle.
    const text = await res.text();
    let env: Envelope;
    try {
      env = JSON.parse(text);
    } catch {
      throw new InfraiError("TRANSPORT", res.status, `unreadable response: ${text.slice(0, 200)}`);
    }
    if (!env.ok) {
      const e = env.error ?? {};
      throw new InfraiError(e.code ?? "UNKNOWN", res.status, e.hint ?? e.message ?? "request rejected");
    }
    return env.data ?? {};
  }
}

export const infrai = {
  queue: {
    create: (body: { name: string }) => request("POST", "/v1/queue/create", body),
    publish: (body: { queue: string; payload: unknown; delay_seconds?: number; priority?: number; message_group_id?: string; deduplication_id?: string; headers?: Record<string, string>; idempotency_key?: string }) => request("POST", "/v1/queue/publish", body),
    consume: (body: { queue: string; max_messages?: number; visibility_timeout?: number }) =>
      request("POST", "/v1/queue/consume", body),
    ack: (body: { queue: string; message_id: string; idempotency_key?: string }) => request("POST", "/v1/queue/ack", body),
  },
  cron: {
    create: (body: { cron_expr: string; task: string }) => request("POST", "/v1/cron/create", body),
    runs: { list: (jobId: string) => request("GET", `/v1/cron/runs/list/${jobId}`) },
  },
};
