# Kafka API constraints

{{ ydb-short-name }} supports [Apache Kafka protocol](https://kafka.apache.org/protocol.html) version 3.4.0 with the following constraints:

1. Only [SASL/PLAIN authentication](https://kafka.apache.org/documentation/#security_sasl) is supported.
1. [Message compression](https://www.confluent.io/blog/apache-kafka-message-compression) is not supported.
1. [The topic deletion operation](https://kafka.apache.org/protocol#The_Messages_DeleteTopics) is not supported. To delete a topic, use [YQL](../../yql/reference/syntax/drop-topic.md) or [{{ ydb-short-name }} CLI](../ydb-cli/topic-drop.md).
1. [CRC checks](https://kafka.apache.org/documentation/#consumerconfigs_check.crcs) are not supported.
1. [Support for ACL](https://kafka.apache.org/documentation/#security_authz) is not provided. Use [YQL](../../yql/reference/syntax/grant.md) to manage access to topics.
1. If [auto-partitioning](../../concepts/datamodel/topic.md#autopartitioning) is enabled on a topic, you cannot write to or read from such a topic using the Kafka API.