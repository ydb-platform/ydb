# YDB Topics

Core implementation of YDB topics (persistent queues).

Shared rules: [`RULES.md`](RULES.md).

## Guidelines

* In `pqrb` / `pqtablet`: batch when persisting; minimize persists and
  inter-actor messages.
* Before changing PQRB read balancing (lock, families, Finish/Commit,
  split/merge, ScaleAwareSDK): read
  [`pqrb/README.md`](pqrb/README.md). That file is the source of truth for
  the intended algorithm; keep it aligned with `read_balancer__balancing.*`.

## Layout

One root directory per tablet or service, plus `common/`, `public/`, `events/`.
In `public/` and `common/`, nested dirs by **purpose**; in `pqrb/` and
`pqtablet/` — by **sub-actor or component**.

* **`public/`** — APIs for code outside persqueue (`schema/`, `fetcher/`, …).
* **`common/`** — shared tablet internals only.
* **`events/`** — internal events and protos.
* **`pqrb/`** — whole-topic tablet (balancing, stats, autopartitioning).
  Read balancing: [`pqrb/README.md`](pqrb/README.md).
* **`pqtablet/`** — one or more partitions (reads, writes, batching, …).
* **`writer/`**, **`dread_cache_service/`**, **`deferred_publish/`** — writer,
  direct-read cache, deferred publish.

Protocol layers and related Topics trees (each has its own `AGENTS.md`):

* [`persqueue_v1`](../../services/persqueue_v1/AGENTS.md) ·
  [`persqueue_v0`](../../services/deprecated/persqueue_v0/AGENTS.md) ·
  [`kafka_proxy`](../kafka_proxy/AGENTS.md) ·
  [`http_proxy`](../http_proxy/AGENTS.md) ·
  [`datastreams`](../../services/datastreams/AGENTS.md) ·
  [`sqs_topic`](../../services/sqs_topic/AGENTS.md) ·
  [`library/persqueue`](../../library/persqueue/AGENTS.md)

## Tests

`./ya make --build relwithdebinfo -tA ydb/core/persqueue` (or a narrower `ut/`).
