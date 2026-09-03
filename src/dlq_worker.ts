/**
 * Runnable path: intake -> work queue -> dead-letter queue -> scheduled sweep.
 *
 * Two Infrai capabilities meet here. The queue carries e-commerce jobs and holds the
 * ones that failed for good; cron owns the clock, POSTing SWEEP_URL on a schedule so
 * the operator inbox gets a digest of dead letters without a long-lived timer process.
 * The handoff is deliberately boring: the sweep endpoint calls the same consume/ack
 * pair the worker uses, only against the dead-letter queue name.
 */
import { infrai, InfraiError } from "./infrai_client.ts";
import { OrderJob, FailureReason, decide, toDeadLetter, type DeadLetter } from "./order_jobs.ts";

export const WORK_QUEUE = "ecommerce-jobs";
export const DEAD_LETTER_QUEUE = "ecommerce-jobs-dead";
const SWEEP_URL = process.env.SWEEP_URL ?? "https://example.com/hooks/dead-letter-sweep";

/** HTTP intake. The body is parsed before anything is published. */
export async function acceptJob(body: unknown): Promise<{ status: number; body: unknown }> {
  const parsed = OrderJob.safeParse(body);
  if (!parsed.success) {
    return { status: 400, body: { error: "invalid job", issues: parsed.error.issues } };
  }
  try {
    // job_id travels inside the payload, so a retried publish is the same job, not a second one.
    const data = await infrai.queue.publish({ queue: WORK_QUEUE, payload: { job: parsed.data } });
    return { status: 202, body: { accepted: parsed.data.job_id, message_id: data.message_id } };
  } catch (err) {
    if (err instanceof InfraiError) {
      // A rejection is a result, not a crash: pass its shape on to our own caller.
      return { status: err.status, body: { error: err.code, detail: err.message } };
    }
    throw err;
  }
}

/** Whatever actually charges the card / books the carrier. Stubbed for the demo run. */
export type Handler = (job: OrderJob) => Promise<{ ok: true } | { ok: false; reason: FailureReason }>;

/** Drain one batch of the work queue, routing exhausted jobs to the dead-letter queue. */
export async function drainOnce(handle: Handler): Promise<{ done: number; retried: number; dead: number }> {
  const batch = await infrai.queue.consume({ queue: WORK_QUEUE, max_messages: 10, visibility_timeout: 60 });
  const messages: Array<{ message_id: string; payload: any }> = batch.messages ?? [];
  let done = 0, retried = 0, dead = 0;

  for (const m of messages) {
    const job = OrderJob.parse(m.payload?.job);
    const result = await handle(job);

    if (result.ok) {
      done++;
    } else {
      const verdict = decide(job, result.reason);
      if (verdict.action === "retry") {
        await infrai.queue.publish({
          queue: WORK_QUEUE,
          payload: { job: { ...job, attempts: verdict.attempts } },
          delay_seconds: verdict.backoff_seconds,
        });
        retried++;
      } else {
        await infrai.queue.publish({
          queue: DEAD_LETTER_QUEUE,
          payload: toDeadLetter(job, result.reason, verdict),
        });
        dead++;
      }
    }
    // Ack once the outcome is durable, so a crash mid-batch replays the job instead of losing it.
    await infrai.queue.ack({ queue: WORK_QUEUE, message_id: m.message_id });
  }
  return { done, retried, dead };
}

/** The scheduled side of the handoff: read the parked jobs so a human can decide. */
export async function sweepDeadLetters(): Promise<DeadLetter[]> {
  const batch = await infrai.queue.consume({ queue: DEAD_LETTER_QUEUE, max_messages: 25, visibility_timeout: 120 });
  const messages: Array<{ message_id: string; payload: any }> = batch.messages ?? [];
  const parked: DeadLetter[] = [];
  for (const m of messages) {
    if (m.payload?.queue === DEAD_LETTER_QUEUE) parked.push(m.payload as DeadLetter);
    await infrai.queue.ack({ queue: DEAD_LETTER_QUEUE, message_id: m.message_id });
  }
  return parked;
}

/** Register the sweep once, at deploy time. Infrai POSTs SWEEP_URL every 15 minutes. */
export async function scheduleSweep(): Promise<string> {
  const job = await infrai.cron.create({ cron_expr: "*/15 * * * *", task: SWEEP_URL });
  return job.job_id;
}

async function main() {
  await infrai.queue.create({ name: WORK_QUEUE });
  await infrai.queue.create({ name: DEAD_LETTER_QUEUE });

  const accepted = await acceptJob({
    job_id: "checkout-4417",
    kind: "checkout",
    order_id: "SO-4417",
    customer_email: "ada@example.com",
    amount_cents: 8900,
    attempts: 0,
  });
  console.log("intake:", accepted.status, accepted.body);

  // A declined card is final on the first pass; the carrier case would be retried.
  const stats = await drainOnce(async (job) =>
    job.kind === "checkout" ? { ok: false, reason: "card_declined" } : { ok: true },
  );
  console.log("drain:", stats);

  const jobId = await scheduleSweep();
  console.log("sweep scheduled:", jobId);
  for (const letter of await sweepDeadLetters()) {
    console.log("parked:", letter.failed_job.order_id, letter.reason, letter.why);
  }
  const runs = await infrai.cron.runs.list(jobId);
  console.log("sweep runs so far:", (runs.items ?? []).length);
}

if (process.argv[1]?.endsWith("dlq_worker.ts")) {
  main().catch((e) => {
    console.error(e instanceof InfraiError ? `${e.code}: ${e.message}` : e);
    process.exit(1);
  });
}
