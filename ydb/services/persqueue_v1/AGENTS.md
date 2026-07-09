# PersQueue v1

gRPC layer for **Topic API** and **PersQueue v1 (PQv1)**.
Core: [`ydb/core/persqueue/AGENTS.md`](../../core/persqueue/AGENTS.md).

## Layout

* Root — entry points (`topic.cpp`, `grpc_pq_*.cpp`, `persqueue.cpp`, init).
* `actors/` — read, write, schema, PQv1 handlers.

Depends on `persqueue/public/` and `events/`, not `pqtablet/` / `pqrb/` internals.

Tests: `./ya make --build relwithdebinfo -tA ydb/services/persqueue_v1`

Style/workflow: [`agents/CODESTYLE.md`](../../agents/CODESTYLE.md) ·
[`agents/AGENTS.md`](../../agents/AGENTS.md) · [`agents/TESTS.md`](../../agents/TESTS.md)
