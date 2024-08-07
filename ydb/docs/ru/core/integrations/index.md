# Интеграции

В данном разделе приведена основная информация про интеграции {{ ydb-name }} со сторонними системами.

## Протоколы доступа {#supported_proto}

|  Протокол | Описание |
| --- | --- |
| [Apache Kafka API](https://kafka.apache.org) | [Описание](../reference/kafka-api/index.md) |
| [PostgreSQL](https://www.postgresql.org) | [Описание](../postgresql/intro.md) |


{% note info %}

В дополнение к своему собственному нативному протоколу, {{ ydb-name }} обладает слоем совместимости, что позволяет внешним системам подключаться к базам данных по сетевым протоколам [PostgreSQL](../postgresql/intro.md) или [Apache Kafka](../reference/kafka-api/index.md). Благодаря слою совместимости, множество инструментов, разработанных для работы с этими системами, могут также взаимодействовать с {{ ydb-name }}. Уровень совместимости каждого конкретного приложения необходимо уточнять отдельно.

{% endnote %}

## SDK {#sdk}

{{ydb-name}} содержит [набор SDK](../reference/ydb-sdk/index.md) для различных языков программирования.


## GUI Clients {#gui}

|  Среда | Инструкция | Уровень поддержки |
| --- | --- | --- |
| Embedded UI | [Инструкция](../reference/embedded-ui/index.md) | |
| [DBeaver](https://dbeaver.com)  |  [Инструкция](ide/dbeaver.md) | C помощью [JDBC-драйвера](https://github.com/ydb-platform/ydb-jdbc-driver/releases)|
| JetBrains Database viewer |  [Инструкция](ide/dbeaver.md)  | C помощью [JDBC-драйвера](https://github.com/ydb-platform/ydb-jdbc-driver/releases)|
| [JetBrains DataGrip](https://www.jetbrains.com/ru-ru/datagrip/) |  [Инструкция](ide/dbeaver.md) | C помощью [JDBC-драйвера](https://github.com/ydb-platform/ydb-jdbc-driver/releases)|
| Другие JDBC-совместимые клиенты | [Инструкция](ide/dbeaver.md) | C помощью [JDBC-драйвера](https://github.com/ydb-platform/ydb-jdbc-driver/releases)|


## Визуализация данных {#bi}

| Среда | Уровень поддержки  | Инструкция |
| --- | :---: | --- |
{% if ydb-datalens %}
| [{{ datalens-name }}](https://datalens.tech/ru) | Полный | [Инструкция](datalens.md) |
{% endif %}
{% if ydb-superset %}

| [Apache Superset](https://superset.apache.org) | Через [PostgreSQL-совместимость](https://ydb.tech/docs/ru/postgresql/intro) | [Инструкция](superset.md) |

{% endif %}
{% if ydb-finebi %}

| [FineBI](https://intl.finebi.com) | Через [PostgreSQL-совместимость](https://ydb.tech/docs/ru/postgresql/intro) | [Инструкция](./finebi.md) |

{% endif %}
| [Grafana](https://grafana.com) | Полный| [Инструкция](grafana.md) |

{% if ydb-airflow %}
## Оркестрация {#orchestration}

| Среда | Инструкция |
| --- | --- |
| [{{ airflow-name }}](https://airflow.apache.org) |   [Инструкция](airflow.md) |

{% endif %}

## Поставка данных {#ingestion}

| Система поставки | Инструкция |
| --- | --- |
| [FluentBit](https://fluentbit.io) | [Инструкция](fluent-bit.md) |
| [LogStash](https://www.elastic.co/logstash) | [Инструкция](logstash.md) |
| [Kafka Connect Sink](https://docs.confluent.io/platform/current/connect/index.html) | [Инструкция](https://github.com/ydb-platform/ydb-kafka-sink-connector) |
| Произвольные [JDBC-источники данных](https://ru.wikipedia.org/wiki/Java_Database_Connectivity) | [Инструкция](import-jdbc.md) |


## Потоковый ввод данных {#streaming-ingestion}

| Система поставки | Инструкция |
| --- | --- |
| [Apache Kafka API](https://kafka.apache.org) | [Инструкция](../reference/kafka-api/index.md) |
| [Apache Kafka Connect](https://kafka.apache.org/documentation/#connect) | [Инструкция](../reference/kafka-api/connect/index.md) |


## Миграции данных {#schema_migration}

| Среда | Инструкция |
| --- | --- |
| [goose](https://github.com/pressly/goose/) | [Инструкция](goose.md) |
| [Liquibase](https://www.liquibase.com) | [Инструкция](liquibase.md) |
| [Flyway](https://documentation.red-gate.com/fd/) | [Инструкция](flyway.md) |
| [Hibernate](https://hibernate.org/orm/) | [Инструкция](hibernate.md) |

## Смотрите также

* [{#T}](../reference/ydb-sdk/index.md)
* [{#T}](../postgresql/intro.md)
* [{#T}](../reference/kafka-api/index.md)
