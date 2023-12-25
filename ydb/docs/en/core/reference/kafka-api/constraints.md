The Kafka protocol version 3.4.0 is supported with limitations:

1. Only authenticated connections allowed.
2. Only SASL/PLAIN authentication supported.
3. Message compression not supported.
4. Only Manual Partition Assignment is supported in read mode, [assign method](https://kafka.apache.org/35/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#assign(java.util.Collection)), without using consumer group partitions.
5. Transactions not supported.
6. DDL transactions not supported. To perform DDL operations, use the [YDB SDK](../ydb-sdk/index.md) or [YDB CLI](../ydb-cli/index.md).
7. Data schema validation not supported.