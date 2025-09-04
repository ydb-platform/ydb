# Kafka API constraints

{{ ydb-short-name }} supports [Apache Kafka protocol](https://kafka.apache.org/protocol.html) version 3.4.0 with the following constraints:

1. Only SASL/PLAIN authentication is supported.
1. Message compression is not supported.
1. The topic deletion operation is not supported. To delete a topic, use [YQL](../../yql/reference/syntax/drop-topic.md) or [{{ ydb-short-name }} CLI](../ydb-cli/topic-drop.md).
1. CRC checks are not supported.
1. Support for ACL is not provided. Use [YQL](../../yql/reference/syntax/grant.md) to manage access to topics.
1. If [auto-partitioning](../../concepts/datamodel/topic.md#autopartitioning) is enabled on a topic, you cannot write to or read from such a topic using the Kafka API.