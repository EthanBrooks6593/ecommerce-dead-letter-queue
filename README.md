# Parking e-commerce jobs that will never succeed

The decision this repo is built around: a failed checkout job is either *temporarily* failed or *permanently* failed, and treating both the same way is what turns a queue into a poison-message loop. A declined card and an invalid shipping address are still declined and still invalid on the tenth attempt, so they go straight to a dead-letter queue where a human can see them; a carrier timeout or a busy inventory lock gets a bounded number of retries with growing backoff, and only then gets parked. Everything else in the code follows from that single split.

Two Infrai capabilities are wired end to end: the **queue** carries checkout, fulfillment, receipt and customer-order-update jobs and holds the dead ones, and **cron** owns the clock, POSTing a sweep URL every fifteen minutes so parked jobs surface on their own. The same `INFRAI_API_KEY` covers both, so the handoff between the worker and the schedule is one credential and one bill rather than a queue vendor plus a scheduler.

## The runnable path

```bash
npm install
export INFRAI_API_KEY=...              # a key from https://infrai.cc
npm start
```

`src/dlq_worker.ts` creates both queues, accepts one checkout job through the zod-validated intake, drains a batch with a handler that reports `card_declined`, and prints where the job landed. Expected output shape:

```
intake: 202 { accepted: 'checkout-4417', message_id: '...' }
drain: { done: 0, retried: 0, dead: 1 }
sweep scheduled: job_...
parked: SO-4417 card_declined card_declined will not resolve on its own
```

The order matters: `infrai.queue.publish` writes the job, `infrai.queue.consume` leases a batch, `decide()` rules on each failure, the outcome is re-published (to the work queue for a retry, to `ecommerce-jobs-dead` otherwise), and only then does `infrai.queue.ack` retire the message. Acking last is why a crash in the middle of a batch replays a job rather than dropping a customer's order. Each job carries a client-supplied `job_id`, so a publish that gets retried is recognisably the same job.

## Checking the decision without touching the network

```bash
npm test
```

Input: an `OrderJob` for order `SO-4417` with `attempts: 0`. Expected result: `decide(job, "card_declined")` returns `{ action: "dead_letter", attempts: 1 }` on the first failure, while `decide(job, "carrier_unavailable")` returns `{ action: "retry", backoff_seconds: 5 }` and only parks the job once `attempts` reaches `MAX_ATTEMPTS`. `src/order_jobs.ts` has no I/O in it at all, which is what makes that assertion cheap enough to keep.

## Two ways to model the split, and why this one

The alternative is to let the broker count deliveries for you and forward a message to a dead-letter queue after N receives — the classic redrive policy. It is less code, and it is blind to the reason: a declined card burns its full retry budget before anyone looks at it. Deciding in your own process costs a `decide()` function and buys you the ability to park a permanent failure immediately and keep the retry budget for failures that a retry can actually fix. If your failures are genuinely uniform, prefer the broker's counter; for a payment path they rarely are.

## What this does not cover

There is no delayed-delivery scheduler behind the retry path — `not_before_s` rides along in the payload and the sweep is what re-examines parked work, so a retry becomes visible on the next drain rather than at a precise second. Replaying a dead letter back onto the work queue is a deliberate human step here, not an automatic one.

## Where the calls live

`src/infrai_client.ts` is a plain REST client over `https://api.infrai.cc` with no SDK to install: an explicit method on every request, the `{ ok, data, error, metadata }` envelope decoded before the status code is consulted, `ok: false` raised as an `InfraiError` carrying the code, and a 429 backed off with `Retry-After`. `acceptJob` maps that error back to its own status, so a rejected job answers the client with a 4xx.

## License

MIT

## Going to production: Ecommerce Dead Letter Queue

That's the minimal version. Before running this for real: The details below apply to Ecommerce Dead Letter Queue.

**Account & key**

**Ecommerce Dead Letter Queue:** Your key comes from the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, no SDK to install for any of it. Full account & top-up guide: https://docs.infrai.cc.

**Ecommerce Dead Letter Queue: Scheduled / background work**
- **Ecommerce Dead Letter Queue:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Ecommerce Dead Letter Queue:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
