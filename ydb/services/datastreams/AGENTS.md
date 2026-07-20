# Data Streams (Kinesis)

**Kinesis-compatible API on top of YDB Topics**.
HTTP entry: [`http_proxy`](../../core/http_proxy/) (`datastreams.cpp`) → this service.
Core: [`ydb/core/persqueue/AGENTS.md`](../../core/persqueue/AGENTS.md).

## Layout

* Root — `datastreams_proxy.cpp`, `grpc_service.cpp`, shard iterators, record writers.
* `codes/` — error codes.
* `ut/` — unit tests.

Depends on `persqueue/public/`, not `pqtablet/` / `pqrb/` internals.

Tests: `./ya make --build relwithdebinfo -tA ydb/services/datastreams`
HTTP integration: `ydb/core/http_proxy/ut/kinesis_ut.cpp`

Style/workflow: [`agents/CODESTYLE.md`](../../agents/CODESTYLE.md) ·
[`agents/GUIDE.md`](../../agents/GUIDE.md) ·
[`agents/GTEST_PREFER.md`](../../agents/GTEST_PREFER.md)
