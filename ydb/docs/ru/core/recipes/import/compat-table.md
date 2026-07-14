# Совместимость инструментов переноса данных

В таблице перечислены способы переноса **данных** из реляционных СУБД и файловых форматов в {{ ydb-short-name }}. Ниже — [рекомендации по выбору инструмента](#how-to-choose): когда нужна готовая таблица, когда схему создаёт утилита, и что лучше для строковых и колоночных таблиц.

| Источник | [YDB CLI](../../reference/ydb-cli/export-import/import-file.md) | [Spark](../../integrations/query-engines/spark.md) | [ydb-importer](../../integrations/data-migration/import-jdbc.md) | [Федеративные запросы](../../concepts/query_execution/federated_query/import_and_export.md) |
| --- | --- | --- | --- | --- |
| [PostgreSQL](from/postgresql/index.md) | — | [да](from/postgresql/spark.md) | [да](from/postgresql/ydb-importer.md) | [да](from/postgresql/federated-queries.md) |
| [Greenplum](from/greenplum/index.md) | — | [да](from/greenplum/spark.md) | [да](from/greenplum/ydb-importer.md) | [да](from/greenplum/federated-queries.md) |
| [MySQL / MariaDB](from/mysql/index.md) | — | [да](from/mysql/spark.md) | [да](from/mysql/ydb-importer.md) | [да](from/mysql/federated-queries.md) |
| [Microsoft SQL Server](from/mssql/index.md) | — | [да](from/mssql/spark.md) | [да](from/mssql/ydb-importer.md) | [да](from/mssql/federated-queries.md) |
| [ClickHouse](from/clickhouse/index.md) | — | [да](from/clickhouse/spark.md) | [да](from/clickhouse/ydb-importer.md) | [да](from/clickhouse/federated-queries.md) |
| [Oracle Database](from/oracle/index.md) | — | [да](from/oracle/spark.md) | [да](from/oracle/ydb-importer.md) | — |
| [CSV](from/csv/index.md) | [да](from/csv/ydb-cli.md) | [да](from/csv/spark.md) | — | — |
| [Parquet](from/parquet/index.md) | [да](from/parquet/ydb-cli.md) | [да](from/parquet/spark.md) | — | [да](from/parquet/federated-queries-s3.md) |

## Как выбрать способ переноса {#how-to-choose}

{% include notitle [Как выбрать способ переноса](tool-guidance.md) %}
