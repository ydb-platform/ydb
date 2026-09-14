# HTTP proxy

HTTP gateway for Topics protocol APIs (**Data Streams / Kinesis**, **SQS**, **YMQ**).
Routes into [`datastreams`](../../services/datastreams/AGENTS.md) and
[`sqs_topic`](../../services/sqs_topic/AGENTS.md).
Core: [`ydb/core/persqueue/AGENTS.md`](../persqueue/AGENTS.md).
Shared rules: [`RULES.md`](../persqueue/RULES.md).

## Layout

* Root — HTTP service, auth, discovery, metrics, JSON/proto conversion;
  entry points `datastreams.cpp`, `sqs.cpp`, `ymq.cpp`.
* `sqs_xml/` — SQS XML request/response helpers.
* `ut/` — HTTP / Kinesis / SQS integration tests.

Tests: `./ya make --build relwithdebinfo -tA ydb/core/http_proxy`
