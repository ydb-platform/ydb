# PersQueue v0 (deprecated)

Legacy gRPC **PersQueue v0** API. Prefer [`persqueue_v1`](../../persqueue_v1/AGENTS.md)
/ Topic API for new work.
Core: [`ydb/core/persqueue/AGENTS.md`](../../../core/persqueue/AGENTS.md).
Shared rules: [`RULES.md`](../../../core/persqueue/RULES.md).

## Layout

* Root — gRPC service entry (`persqueue.*`), read/write session actors.
* `api/` — gRPC and protos for PersQueue v0.

Tests: build with dependents; no dedicated top-level `ut/` here.
