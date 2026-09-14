# PersQueue v1

gRPC layer for **Topic API** and **PersQueue v1 (PQv1)**.
Core: [`ydb/core/persqueue/AGENTS.md`](../../core/persqueue/AGENTS.md).
Shared rules: [`RULES.md`](../../core/persqueue/RULES.md).

## Layout

* Root — entry points (`topic.cpp`, `grpc_pq_*.cpp`, `persqueue.cpp`, init).
* `actors/` — read, write, schema, PQv1 handlers.

Tests: `./ya make --build relwithdebinfo -tA ydb/services/persqueue_v1`
