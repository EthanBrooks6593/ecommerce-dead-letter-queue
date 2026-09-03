// The reusable half of the example: the job vocabulary and the retry/dead-letter
// decision. Nothing here talks to the network, which is what makes it testable.
import { z } from "zod";

export const JobKind = z.enum(["checkout", "fulfillment", "receipt", "order_update"]);
export type JobKind = z.infer<typeof JobKind>;

// Request bodies arrive from HTTP handlers and from the queue alike, so they are
// parsed with the same schema on both sides of the handoff.
export const OrderJob = z.object({
  job_id: z.string().min(1), // client-supplied, so a retried publish never double-applies
  kind: JobKind,
  order_id: z.string().min(1),
  customer_email: z.string().email(),
  amount_cents: z.number().int().nonnegative(),
  attempts: z.number().int().nonnegative().default(0),
});
export type OrderJob = z.infer<typeof OrderJob>;

export const FailureReason = z.enum([
  "card_declined",
  "address_invalid",
  "carrier_unavailable",
  "smtp_temporary",
  "inventory_lock_timeout",
]);
export type FailureReason = z.infer<typeof FailureReason>;

// A declined card and a bad shipping address will still be declined and still be bad
// on the tenth try; a carrier hiccup or a busy inventory lock will not. Sorting the
// two apart is the whole reason a dead-letter queue earns its keep.
const PERMANENT: ReadonlySet<FailureReason> = new Set<FailureReason>(["card_declined", "address_invalid"]);

export const MAX_ATTEMPTS = 3;

export type Verdict =
  | { action: "retry"; attempts: number; backoff_seconds: number }
  | { action: "dead_letter"; attempts: number; why: string };

export function decide(job: OrderJob, reason: FailureReason): Verdict {
  const attempts = job.attempts + 1;
  if (PERMANENT.has(reason)) {
    return { action: "dead_letter", attempts, why: `${reason} will not resolve on its own` };
  }
  if (attempts >= MAX_ATTEMPTS) {
    return { action: "dead_letter", attempts, why: `${reason} survived ${attempts} attempts` };
  }
  return { action: "retry", attempts, backoff_seconds: 5 * 2 ** (attempts - 1) };
}

export type DeadLetter = {
  dead_letter: true;
  failed_job: OrderJob;
  reason: FailureReason;
  attempts: number;
  why: string;
};

export function toDeadLetter(job: OrderJob, reason: FailureReason, v: Verdict & { action: "dead_letter" }): DeadLetter {
  return { dead_letter: true, failed_job: job, reason, attempts: v.attempts, why: v.why };
}
