# Интеграции

В данном разделе приведена основная информация про интеграции {{ ydb-name }} со сторонними системами.

## Протоколы доступа {#supported_proto}

|  Протокол | Описание |
| --- | --- |
| [Apache Kafka API](https://kafka.apache.org) | [Описание](../reference/kafka-api/index.md) |
| [PostgreSQL](https://www.postgresql.org) | [Описание](../postgresql/intro.md) |


{% note info %}

{{ ydb-name }} обладает [слоем совместимости с PostgreSQL](../postgresql/intro.md). Благодаря этой интеграции большое число инструментов могут взаимодействовать с {{ ydb-name }}. Уровень совместимости каждого отдельного приложения необходимо уточнять в каждом конкретном случае.

{% endnote %}

## SDK {#sdk}

|  Библиотеки  | Язык программирования  | Репозиторий |
| --- | :---:|--- |
| database/sql | Go | [Репозиторий](https://github.com/ydb-platform/ydb-go-sdk/blob/master/SQL.md) |
| ydb-go-sdk | Go |  [Репозиторий](https://github.com/ydb-platform/ydb-go-sdk) |
| JDBC | Java |  [Репозиторий](https://github.com/ydb-platform/ydb-jdbc-driver) |
| ydb-java-sdk | Java |  [Репозиторий](https://github.com/ydb-platform/ydb-java-sdk) |
| ydb-python-sdk | Python |  [Репозиторий](https://github.com/ydb-platform/ydb-python-sdk) |
| ydb-php-sdk | PHP |  [Репозиторий](https://github.com/ydb-platform/ydb-php-sdk) |
| ydb-cpp-sdk | C++ |  [Репозиторий](https://github.com/ydb-platform/ydb-cpp-sdk) |
| ydb-dotnet-sdk | .Net |  [Репозиторий](https://github.com/ydb-platform/ydb-dotnet-sdk) |
| ydb-rs-sdk | Rust |  [Репозиторий](https://github.com/ydb-platform/ydb-rs-sdk) |

## GUI Clients {#gui}

|  Среда | Инструкция | Уровень поддержки |
| --- | --- | --- |
| Embedded UI | [Инструкция](../reference/embedded-ui/index.md) | |
| [DBeaver](https://dbeaver.com)  |  [Инструкция](connect-from-ide.md) | C помощью [JDBC-драйвера](https://github.com/ydb-platform/ydb-jdbc-driver/releases)|
| JetBrains Database viewer |  [Инструкция](connect-from-ide.md)  | C помощью [JDBC-драйвера](https://github.com/ydb-platform/ydb-jdbc-driver/releases)|
| [JetBrains DataGrip](https://www.jetbrains.com/ru-ru/datagrip/) |  [Инструкция](connect-from-ide.md) | C помощью [JDBC-драйвера](https://github.com/ydb-platform/ydb-jdbc-driver/releases)|
| Другие JDBC-совместимые клиенты | [Инструкция](connect-from-ide.md) | C помощью [JDBC-драйвера](https://github.com/ydb-platform/ydb-jdbc-driver/releases)|


## Визуализация данных {#bi}

| Среда | Уровень поддержки  | Инструкция |
| --- | :---: | --- |
{% if ydb-datalens %}
| [{{ datalens-name }}](https://datalens.tech/ru) | Полный | [Инструкция](datalens.md) |
{% endif %}
{% if ydb-finebi %}

| [Apache Superset](https://superset.apache.org) | Через [PostgreSQL-совместимость](https://ydb.tech/docs/ru/postgresql/intro) | [Инструкция](superset.md) |

{% endif %}
{% if ydb-finebi %}

| [FineBI](https://intl.finebi.com) | Через [PostgreSQL-совместимость](https://ydb.tech/docs/ru/postgresql/intro) | [Инструкция](./finebi.md) |

{% endif %}
| [Grafana](https://grafana.com) | Полный| [Инструкция](grafana.md) |

{% if ydb-airflow %}
## Оркестрация {#scheduling}

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
| [goose](https://github.com/pressly/goose/blob/master/README.md) | [Инструкция](goose.md) |
| [Liquibase](https://www.liquibase.com) | [Инструкция](liquibase.md) |
| [Flyway](https://documentation.red-gate.com/fd/) | [Инструкция](flyway.md) |
| [Hibernate](https://hibernate.org/orm/) | [Инструкция](hibernate.md) |


