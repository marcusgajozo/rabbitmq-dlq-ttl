# RabbitMQ retry with TTL and dead letter queue

A study project about one question: **how do you retry a failed message with increasing delays in RabbitMQ, without blocking the queue?**

The repository contains two experiments. The first, `naive.js`, builds the intuitive solution, one waiting queue with a per-message TTL, and shows why it does not work. The second, `topologia.js` plus `consumidor.js`, builds the solution that does: one queue per delay step, each with its own TTL, dead-lettering back to the main exchange.

## The trap: per-message TTL in a single queue

The obvious idea is a queue called `waiting` where each message carries its own `expiration`. A message that should wait ten minutes gets `600000`, one that should wait one minute gets `60000`.

The problem is that **a queue delivers in order**. RabbitMQ only expires the message at the head. If the ten-minute message arrives first, the one-minute message behind it waits ten minutes too, because it cannot be delivered before the head is gone.

`naive.js` reproduces exactly this: message A with a ten-minute TTL is published first, message B with a one-minute TTL right after. B leaves after ten minutes, not after one.

The lesson: **per-message TTL gives you the delay you asked for only if messages expire in the order they arrived**, which is never guaranteed in a retry flow.

## The fix: one queue per delay step

```text
                   ex.appointments (topic)
                            │ appointment.*
                            ▼
              ┌──────────────────────────────┐
              │ notifications.appointment    │
              │ prefetch 1, manual ack       │
              └──────────────┬───────────────┘
                 failure     │
                             ▼
                   ex.retry (direct)
        ┌──────────────┬──────────────┬──────────────┐
        │ retry.1      │ retry.2      │ retry.3      │
        ▼              ▼              ▼              ▼
  notifications   notifications  notifications  notifications
  .retry.30s      .retry.2m      .retry.10m     .parking
  TTL 30s         TTL 2m         TTL 10m        (no TTL)
        │              │              │
        └──────────────┴──────────────┘
             dead letter back to
             ex.appointments / appointment.created
```

Each delay is a **queue-level TTL**, so every message inside that queue expires at the same moment relative to its own arrival. The delay no longer depends on what is in front of it.

When the TTL expires, the dead letter exchange sends the message back to the main exchange with the routing key `appointment.created`, and the main queue sees it again. This is the classic **delay queue** built from TTL plus DLX.

After the last step, the consumer stops retrying and publishes to `notifications.parking`, a queue with no TTL that keeps the message for a human to inspect. This is the dead letter queue of the flow.

## What each file does

- `topologia.js` declares the two exchanges, the main queue with its dead letter routing, the three retry queues with their TTLs, and the parking queue.
- `produtor.js` publishes an `appointment.created` message to the main exchange.
- `consumidor.js` consumes with `prefetch(1)`. It reads the `x-attempts` header, and on failure publishes to the next step, incrementing the counter. On the third attempt it sends the message to the parking queue.
- `naive.js` is the counterexample: two messages with different per-message TTLs in a single queue, to watch the head-of-line blocking happen in the management panel.

## Running it

```bash
pnpm install
docker compose up -d
node topologia.js          # creates exchanges, queues and bindings
node consumidor.js         # keep it running in a terminal
node produtor.js           # in another terminal, publishes one message
```

The management panel runs at `http://localhost:15672` with `manicure` / `manicure`. Watch `notifications.appointment` and the three retry queues while the consumer rejects the message: the counter goes up, the message walks through the steps and ends in `notifications.parking`.

To run the counterexample:

```bash
node naive.js
```

Then open `espera.unica` in the panel and compare the arrival order with the two expiration values.

## What this project shows

- A retry flow with backoff is a **topology problem**, not a loop with `setTimeout` inside the consumer.
- Queue-level TTL is predictable. Per-message TTL is not, once the queue holds messages with different delays.
- The dead letter exchange is what makes the delay queue work, and it is the same mechanism that gives you the parking queue at the end.
- `prefetch(1)` keeps the failure of one message from dragging a batch with it.
- The parking queue is where a message goes when retrying stopped being useful. Something has to look at it, and that something is a person or a job, not the consumer.

## Known details

- `produtor.js` writes the header as `x-attemps`, while the consumer reads `x-attempts`. The first message therefore starts at attempt 1 by default, which masks the typo. Worth fixing if this becomes more than a study.
- The retry budget is defined in `consumidor.js` as `MAX_TENTATIVAS`, and the steps are defined in `topologia.js` as `DEGRAUS`. The two lists have to agree.

## Reading

- RabbitMQ, [Time-to-live and expiration](https://www.rabbitmq.com/docs/ttl)
- RabbitMQ, [Dead lettering](https://www.rabbitmq.com/docs/dlx)
- RabbitMQ, [Consumer prefetch](https://www.rabbitmq.com/docs/consumer-prefetch)
