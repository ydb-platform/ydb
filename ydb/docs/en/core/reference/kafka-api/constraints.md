# Kafka API constraints

YDB supports [Apache Kafka protocol](https://kafka.apache.org/protocol.html) version 3.4.0 with the following constraints:

1. Only authenticated connections are allowed.

2. Only `SASL/PLAIN` authentication method is supported.

3. Message compression is not supported.

4. Only Manual Partition Assignment is supported in read mode, using the [assign method](https://kafka.apache.org/35/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#assign(java.util.Collection)). Consumer group partitions can't be used.

5. Transactions are not supported.

6. DDL operations are not supported. Use the [YDB SDK](../ydb-sdk/index.md) or [YDB CLI](../ydb-cli/index.md) to perform them.

7. Data schema validation not supported.