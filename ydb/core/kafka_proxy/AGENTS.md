# Kafka proxy

**Kafka-compatible API on top of YDB Topics**.
Accepts **Kafka wire-protocol** connections (`kafka_connection.cpp`).
Core: [`ydb/core/persqueue/AGENTS.md`](../persqueue/AGENTS.md).

## Layout

* Root — connections, message codecs (`kafka_messages.*`), transactions.
* `actors/` — produce, fetch, metadata, offsets, consumer groups, SASL, …

Depends on `persqueue/public/`, not `pqtablet/` / `pqrb/` internals.

Tests: `./ya make --build relwithdebinfo -tA ydb/core/kafka_proxy`

Style/workflow: [`agents/CODESTYLE.md`](../../agents/CODESTYLE.md) ·
[`agents/GUIDE.md`](../../agents/GUIDE.md) ·
[`agents/GTEST_PREFFER.md`](../../agents/GTEST_PREFFER.md)
