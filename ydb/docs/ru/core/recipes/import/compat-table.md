# Совместимость инструментов переноса данных

В таблице перечислены способы переноса **данных** из реляционных СУБД и файловых форматов в {{ ydb-short-name }}. Ниже — [рекомендации по выбору инструмента](#how-to-choose): когда нужна готовая таблица, когда схему создаёт утилита, и что лучше для строковых и колоночных таблиц.

| Источник | [YDB CLI](../../reference/ydb-cli/export-import/import-file.md) | [Spark](../../integrations/query-engines/spark.md) | [ydb-importer](../../integrations/data-migration/import-jdbc.md) | [Федеративные запросы](../../concepts/query_execution/federated_query/import_and_export.md) |
| --- | --- | --- | --- | --- |
| [PostgreSQL](from/postgresql/index.md) | [CSV](from/postgresql/cli-import-file.md), [Parquet](from/postgresql/cli-import-parquet.md) | [да](from/postgresql/spark.md) | [да](from/postgresql/ydb-importer.md) | [да](from/postgresql/federated-queries.md) |
| [Greenplum](from/greenplum/index.md) | [CSV](from/greenplum/cli-import-file.md), [Parquet](from/greenplum/cli-import-parquet.md) | [да](from/greenplum/spark.md) | [да](from/greenplum/ydb-importer.md) | [да](from/greenplum/federated-queries.md) |
| [MySQL / MariaDB](from/mysql/index.md) | [CSV](from/mysql/cli-import-file.md), [Parquet](from/mysql/cli-import-parquet.md) | [да](from/mysql/spark.md) | [да](from/mysql/ydb-importer.md) | [да](from/mysql/federated-queries.md) |
| [Microsoft SQL Server](from/mssql/index.md) | [CSV](from/mssql/cli-import-file.md), [Parquet](from/mssql/cli-import-parquet.md) | [да](from/mssql/spark.md) | [да](from/mssql/ydb-importer.md) | [да](from/mssql/federated-queries.md) |
| [ClickHouse](from/clickhouse/index.md) | [CSV](from/clickhouse/cli-import-file.md), [Parquet](from/clickhouse/cli-import-parquet.md) | [да](from/clickhouse/spark.md) | [да](from/clickhouse/ydb-importer.md) | [да](from/clickhouse/federated-queries.md) |
| [Oracle Database](from/oracle/index.md) | [CSV](from/oracle/cli-import-file.md), [Parquet](from/oracle/cli-import-parquet.md) | [да](from/oracle/spark.md) | [да](from/oracle/ydb-importer.md) | — |
| CSV | [да](../../reference/ydb-cli/export-import/import-file.md) | [да](../../integrations/query-engines/spark.md) | — | — |
| Parquet | [да](../../reference/ydb-cli/export-import/import-file.md) | [да](../../integrations/query-engines/spark.md) | — | — |

## Как выбрать способ переноса {#how-to-choose}

{% include notitle [Как выбрать способ переноса](tool-guidance.md) %}
