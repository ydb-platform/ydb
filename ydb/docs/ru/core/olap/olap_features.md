# Основные функции аналитической обработки в {{ydb-short-name}}

## Хранение данных в колоночных таблицах

- [Выбор ключей для колоночных таблиц](../dev/primary-key/column-oriented).
- [Партиционирование](../concepts/datamodel/table.md#olap-tables-partitioning) данных.

## SQL и аналитические функции

- [Язык запросов](../yql/reference/index.md).
- [Типовые сценарии работы с датами](../yql/reference/udf/list/datetime.md#tipovye-scenarii).
- [Оконные функции](../yql/reference/builtins/window.md).


## Оптимизация запросов

- [!!!ссылка на описание оптимизатора](../yql/reference/index.md).

Распределённые JOIN-ы. Планирование обменов данными между узлами, стратегии перераспределения.
Где читать: Концепции → Распределённые операторы; Руководство → Соединения в MPP.


## Управление нагрузкой

- [Ресурсные пулы](../dev/resource-consumption-management.md)

## Загрузка и интеграция данных

- Потоковый ввод данных с использованием [топиков](../concepts/topic.md) с поддержкой [Kafka протокола](../reference/kafka-api/index.md) и [транзакций между топиками и таблицами](../concepts/transactions.md#topic-table-transactions).
- Автоматический перенос данных между топиками и таблицами с использованием [!!!TRANSFER](.).
- [!!!!Потоковый перенос CDC-данных](SCD1) между строковыми и колоночными таблицами.

Поставка данных с использованием готовых коннекторов:

- [Apache Spark](../integrations/ingestion/spark);
- [FluentBit](../integrations/ingestion/fluent-bit);
- [и других](../integrations/ingestion/index.md).

### Федеративные запросы

С помощью федеративных запросов можно выполнять прямые запросы к внешним источникам данных:

- [PostgreSQL](../concepts/federated_query/postgresql.md);
- [ClickHouse](../concepts/federated_query/clickhouse.md);
- [S3](../concepts/federated_query/s3/external_table.md);

## Инструменты инженера данных

Для работы инженеров данных {{ydb-short-name}} предоставляет ряд готовых коннекторов:

- [!!!!DBT](.)
- [Apache Airflow](../integrations/orchestration/airflow.md).

Работа с данными в формате SCD (slow changing dimensions) описана в разделе:

- [!!!SCD2](.). # !!!../dataeng/scd2.md

## Инструменты аналитика

Анализ данных с помощью YDB можно производить с помощью готовых интеграций с:

- [!!!Apache SuperSet](.); # ../integrations/gui/!!
- [Grafana](../integrations/visualization/grafana.md).
- [Jupyter Notebooks](../integrations/gui/jupyter.md)
- и [другие](../integrations/index.md).

С YDB можно взаимодействовать с помощью MCP:

- [MCP Server](../reference/languages-and-apis/mcp/index.md).

## ML/ETL/ELT

Драйвер для [Apache Spark](../integrations/ingestion/spark) с параллельным чтением данных, позволяющим получать высокую скорость чтения данных из {{ydb-short-name}}.

SDK/JDBC/CLI. Драйверы и инструменты для разработки и автоматизации.

- поддерживается более 8 языков программирования в [{{ydb-short-name}} native SDK](../reference/ydb-sdk/index.md).
- [JDBC](../reference/languages-and-apis/jdbc-driver/index.md)
- [и другие](../reference/languages-and-apis/index.md)


## Мониторинг и профилирование. Метрики кластера, планы, профили операторов, трассировка

[Для диагностики](../troubleshooting/performance/index.md) доступен набор инструментов:

- [Встроенная диагностика {{ydb-short-name}}](../reference/embedded-ui/index.md), обнаруживающая и предоставляющая необходимую информацию для диагностирования работы кластера.
- [Метрики](../reference/observability/metrics/index.md)
- Готовые [дешборды Grafana](../reference/observability/metrics/grafana-dashboards.md).

## Резервное копирование и восстановление. Политики snapshot/backup, тест восстановления

В данный момент бэкап встроенными средствами не поддерживается. Вместо этого можно выполнять копирование и восстановление данных с помощью [федеративных запросов к S3](../concepts/federated_query/s3/external_table.md).

## Безопасность и доступ. Роли и права, интеграция с IAM, аудит

Доступны различные варианты авторизации:

- [Логин-пароль](../security/authentication.md#static-credentials)
- [LDAP](../security/authentication.md#static-credentials#ldap).
- [и другие методы](../security/authentication.md).

