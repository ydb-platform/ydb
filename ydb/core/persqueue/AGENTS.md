# YDB Topics

Core implementation of YDB topics (persistent queues).

## Layout

One root directory per tablet or service, plus `common/`, `public/`, `events/`.
In `public/` and `common/`, nested dirs by **purpose**; in `pqrb/` and
`pqtablet/` — by **sub-actor or component**.

* **`public/`** — APIs for code outside persqueue (`schema/`, `fetcher/`, …).
* **`common/`** — shared tablet internals only.
* **`events/`** — internal events and protos.
* **`pqrb/`** — whole-topic tablet (balancing, stats, autopartitioning).
* **`pqtablet/`** — one or more partitions (reads, writes, batching, …).
* **`writer/`**, **`dread_cache_service/`**, **`deferred_publish/`** — writer,
  direct-read cache, deferred publish.

Protocol layers (each has its own `AGENTS.md`):

* [`persqueue_v1`](../../services/persqueue_v1/AGENTS.md) ·
  [`kafka_proxy`](../kafka_proxy/AGENTS.md) ·
  [`datastreams`](../../services/datastreams/AGENTS.md) ·
  [`sqs_topic`](../../services/sqs_topic/AGENTS.md)

## Guidelines

* External code uses `public/` (and `events/` where needed), not `pqtablet/` /
  `pqrb/` internals. Layering: `public` → tablets.
* In `pqrb` / `pqtablet`: batch when persisting; minimize persists and
  inter-actor messages; extract large logic into separate actors or classes.
* `*_fwd.h` for forward declarations; `.pb.h` only when needed.
  Config helpers: [`public/config.h`](public/config.h).
* Put method implementations in `.cpp` files, not in headers — except
  template functions and template classes.

## Tests

`./ya make --build relwithdebinfo -tA ydb/core/persqueue` (or a narrower `ut/`).
Monorepo rules: [`agents/CODESTYLE.md`](../../agents/CODESTYLE.md) ·
[`agents/GUIDE.md`](../../agents/GUIDE.md) · [`agents/TESTS.md`](../../agents/TESTS.md).
