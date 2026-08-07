# SQS over Topics

**SQS API on top of YDB Topics**.
HTTP entry: [`http_proxy`](../../core/http_proxy/) (`sqs.cpp`) → this service.
Core: [`ydb/core/persqueue/AGENTS.md`](../../core/persqueue/AGENTS.md).
Shared rules: [`RULES.md`](../../core/persqueue/RULES.md).

## Layout

* Root — API handlers and `sqs_topic_proxy.cpp`.
* `queue_url/` — URL parsing (`arn`, `consumer`, `holder/`).
* `protos/` — SQS protos (e.g. receipt).
* `ut/` — unit tests.

Tests: `./ya make --build relwithdebinfo -tA ydb/services/sqs_topic`
Functional: `ydb/tests/functional/sqs/`
