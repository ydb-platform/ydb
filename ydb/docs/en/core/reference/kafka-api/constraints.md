# Kafka API constraints

{{ ydb-short-name }} supports [Apache Kafka protocol](https://kafka.apache.org/protocol.html) version 3.4.0 with the following constraints:

1. Only SASL/PLAIN authentication is supported.
1. [Compacted topics](https://docs.confluent.io/kafka/design/log_compaction.html) are not supported. Consequently, Kafka Connect, Schema Registry, and Kafka Streams do not work over the Kafka API in YDB Topics.
1. Message compression is not supported.
1. Transactions are not supported.
1. DDL operations are not supported. For DDL operations, use [{{ ydb-short-name }} SDK](../ydb-sdk/index.md) or [{{ ydb-short-name }} CLI](../ydb-cli/index.md).
1. CRC checks are not supported.
1. Kafka Connect works only in a standalone mode (single-worker mode).
1. If auto-partitioning is enabled on a topic, you cannot write to or read from such a topic using the Kafka API.