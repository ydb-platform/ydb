# Kafka API constraints

YDB supports [Apache Kafka protocol](https://kafka.apache.org/protocol.html) version 3.4.0 with the following constraints:

1. Only authenticated connections are allowed.

2. Only `SASL/PLAIN` authentication method is supported.

3. Message compression is not supported.

4. Transactions are not supported.

5. DDL operations are not supported. Use the [YDB SDK](../ydb-sdk/index.md) or [YDB CLI](../ydb-cli/index.md) to perform them.

6. Data schema validation not supported.

7. Kafka Connect is only supported in standalone mode.