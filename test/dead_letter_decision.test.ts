import test from "node:test";
import assert from "node:assert/strict";
import { OrderJob, decide, MAX_ATTEMPTS } from "../src/order_jobs.ts";

const base = OrderJob.parse({
  job_id: "checkout-4417",
  kind: "checkout",
  order_id: "SO-4417",
  customer_email: "ada@example.com",
  amount_cents: 8900,
});

test("a declined card is parked on the first failure", () => {
  const v = decide(base, "card_declined");
  assert.equal(v.action, "dead_letter");
  assert.equal(v.attempts, 1);
});

test("a carrier hiccup is retried with growing backoff", () => {
  const first = decide(base, "carrier_unavailable");
  assert.equal(first.action, "retry");
  assert.equal(first.action === "retry" && first.backoff_seconds, 5);

  const second = decide({ ...base, attempts: 1 }, "carrier_unavailable");
  assert.equal(second.action === "retry" && second.backoff_seconds, 10);
});

test("a transient failure is parked once the attempt budget runs out", () => {
  const v = decide({ ...base, attempts: MAX_ATTEMPTS - 1 }, "smtp_temporary");
  assert.equal(v.action, "dead_letter");
  assert.equal(v.attempts, MAX_ATTEMPTS);
});

test("intake rejects a body that is not an order job", () => {
  assert.equal(OrderJob.safeParse({ job_id: "x", kind: "refund" }).success, false);
});
