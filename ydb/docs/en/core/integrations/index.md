# Integrations {{ ydb-short-name }}

This section provides the main information about {{ ydb-name }} integrations with third-party systems.

{% note info %}

In addition to its own native protocol, {{ ydb-name }} has a compatibility layer that allows external systems to connect to databases via network protocols PostgreSQL or Apache Kafka. Due to the compatibility layer, many tools designed to work with these systems can also interact with {{ ydb-name }}. The compatibility level of each specific application needs to be clarified separately.

{% endnote %}


## Graphical Interface Clients {#gui}

|  Environment | Instruction | Compatibility Level |
| --- | --- | --- |
| Embedded UI | [Instruction](../reference/embedded-ui/index.md) | |
| [DBeaver](https://dbeaver.com)  |  [Instruction](ide/dbeaver.md) | By [JDBC-driver](https://github.com/ydb-platform/ydb-jdbc-driver/releases)|
| JetBrains Database viewer |  —  | By [JDBC-driver](https://github.com/ydb-platform/ydb-jdbc-driver/releases)|
| [JetBrains DataGrip](https://www.jetbrains.com/datagrip/) |  [Instruction](ide/datagrip.md) | By [JDBC-driver](https://github.com/ydb-platform/ydb-jdbc-driver/releases)|
| Other JDBC-compatible IDEs | — | By [JDBC-driver](https://github.com/ydb-platform/ydb-jdbc-driver/releases)|


## Data Visualization (Business Intelligence, BI) {#bi}

| Environment | Compatibility Level  | Instruction |
| --- | :---: | --- |
| [Grafana](https://grafana.com) | Full| [Instruction](grafana.md) |


## Data Ingestion {#ingestion}

| Delivery System | Instruction |
| --- | --- |
| [FluentBit](https://fluentbit.io) | [Instruction](fluent-bit.md) |
| [LogStash](https://www.elastic.co/logstash) | [Instruction](logstash.md) |
| [Kafka Connect Sink](https://docs.confluent.io/platform/current/connect/index.html) | [Instruction](https://github.com/ydb-platform/ydb-kafka-sink-connector) |
| Arbitrary [JDBC-источники данных](https://en.wikipedia.org/wiki/Java_Database_Connectivity) | [Instruction](import-jdbc.md) |


### Streaming Data Ingestion {#streaming-ingestion}

| Delivery System | Instruction |
| --- | --- |
| [Apache Kafka API](https://kafka.apache.org) | [Instruction](../reference/kafka-api/index.md) |


## Data Migrations {#schema_migration}

| Environment | Instruction |
| --- | --- |
| [goose](https://github.com/pressly/goose/) | [Instruction](goose.md) |
| [Liquibase](https://www.liquibase.com) | [Instruction](liquibase.md) |
| [Flyway](https://documentation.red-gate.com/fd/) | [Instruction](flyway.md) |
| [Hibernate](https://hibernate.org/orm/) | [Instruction](hibernate.md) |

## See Also

* [{#T}](../reference/ydb-sdk/index.md)
* [{#T}](../postgresql/intro.md)
* [{#T}](../reference/kafka-api/index.md)
