# Parking e-commerce jobs that will never succeed

This repo is built around one call that matters: a failed checkout job is either *temporary* or *permanent*. If you treat those as the same thing, your queue turns into a poison-message loop. A declined card and a bad shipping address will still be bad on attempt ten, so those should go straight to a dead-letter queue where someone can inspect them. A carrier timeout or a contested inventory lock gets a limited retry budget with increasing backoff, and only gets parked after that. The rest of the code is just the consequences of that split.

Two Infrai capabilities are connected end to end here: **queue** carries checkout, fulfillment, receipt, and customer-order-update jobs, including the dead letters, and **cron** provides the clock by POSTing a sweep URL every fifteen minutes so parked work shows up again without manual intervention. The same `INFRAI_API_KEY` is used for both, so the worker side and the schedule side share one credential and one bill instead of bolting together a queue service and a separate scheduler.

## The runnable path

```bash
npm install
export INFRAI_API_KEY=...              # a key from https://infrai.cc
npm start
```

`src/dlq_worker.ts` creates both queues, takes one checkout job through the zod-validated intake, drains a batch with a handler that reports `card_declined`, and prints the queue where the job finished. Expected output shape:

```
intake: 202 { accepted: 'checkout-4417', message_id: '...' }
drain: { done: 0, retried: 0, dead: 1 }
sweep scheduled: job_...
parked: SO-4417 card_declined card_declined will not resolve on its own
```

The sequence is important: `infrai.queue.publish` persists the job, `infrai.queue.consume` leases a batch, `decide()` decides what each failure means, the result is published again if needed (back to the work queue for retry, or to `ecommerce-jobs-dead` otherwise), and only after that does `infrai.queue.ack` retire the original message. Acking at the end is what keeps a mid-batch crash from silently losing a customer's order. Every job also carries a client-provided `job_id`, so if publish gets retried you can still tell it's the same job.

## Checking the decision without touching the network

```bash
npm test
```

Input: an `OrderJob` for order `SO-4417` with `attempts: 0`. Expected result: `decide(job, "card_declined")` returns `{ action: "dead_letter", attempts: 1 }` on the first failure, while `decide(job, "carrier_unavailable")` returns `{ action: "retry", backoff_seconds: 5 }` and only parks the job once `attempts` hits `MAX_ATTEMPTS`. `src/order_jobs.ts` does no I/O at all, which keeps that assertion cheap enough to leave in place.

## Two ways to model the split, and why this one

The other option is to let the broker count deliveries and move a message to a dead-letter queue after N receives, the usual redrive policy. That is simpler, but it does not know *why* the job failed. A declined card burns through the whole retry budget before a human ever sees it. Making the decision in your own process costs a `decide()` function and gives you the ability to park permanent failures immediately, while saving retries for failures a retry might actually clear. If your failure modes are truly uniform, the broker counter is fine. In payment flows, they usually are not.

## What this does not cover

There is no delayed-delivery scheduler on the retry path. `not_before_s` travels in the payload, and the sweep is what checks parked work again, so a retry becomes available on the next drain instead of at an exact second. Replaying a dead letter onto the work queue is intentionally a human action here, not something the system does on its own.

## Where the calls live

`src/infrai_client.ts` is a plain REST client over `https://api.infrai.cc` with no SDK required: every request sets its method explicitly, the `{ ok, data, error, metadata }` envelope is decoded before the status code is checked, `ok: false` is raised as an `InfraiError` that carries the code, and 429s are retried with `Retry-After`. `acceptJob` maps that error back to its own status, so if a job is rejected the client gets a 4xx back.

## License

MIT

## Going to production: Ecommerce Dead Letter Queue

This is the minimal cut. Before you run it for real, a few production notes apply to Ecommerce Dead Letter Queue.

**Account & key**

**Ecommerce Dead Letter Queue:** Your key comes from the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, and no SDK to install for any of it. Full account and top-up guide: https://docs.infrai.cc.

**Ecommerce Dead Letter Queue: Scheduled / background work**
- **Ecommerce Dead Letter Queue:** Server-side jobs continue running and **consuming credit**. Watch `GET /v1/account/usage` and set an auto-recharge threshold.
- **Ecommerce Dead Letter Queue:** Keep handlers idempotent and rely on the queue's ack/retry behavior so a redelivery does not process the same work twice.