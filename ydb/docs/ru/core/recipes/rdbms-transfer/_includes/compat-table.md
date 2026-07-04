# Совместимость инструментов переноса данных

В таблице перечислены способы переноса **данных** из реляционных СУБД в {{ ydb-short-name }}. Перенос схемы в scope статей не входит — предполагается, что целевые таблицы в {{ ydb-short-name }} уже созданы (или создаются инструментом вместе с данными, если это его штатное поведение).

| Инструмент ↓ / СУБД → | [PostgreSQL](../from/postgresql/index.md) | [Greenplum](../from/greenplum/index.md) | [MySQL / MariaDB](../from/mysql/index.md) | [Microsoft SQL Server](../from/mssql/index.md) | [ClickHouse](../from/clickhouse/index.md) | [Oracle Database](../from/oracle/index.md) |
| --- | --- | --- | --- | --- | --- | --- |
| [CLI import file](../tools/cli-import-file.md) | [да](../from/postgresql/cli-import-file.md) | [да](../from/greenplum/cli-import-file.md) | [да](../from/mysql/cli-import-file.md) | [да](../from/mssql/cli-import-file.md) | [да](../from/clickhouse/cli-import-file.md) | [да](../from/oracle/cli-import-file.md) |
| [Spark](../tools/spark.md) | [да](../from/postgresql/spark.md) | [да](../from/greenplum/spark.md) | [да](../from/mysql/spark.md) | [да](../from/mssql/spark.md) | [да](../from/clickhouse/spark.md) | [да](../from/oracle/spark.md) |
| [ydb-importer](../tools/ydb-importer.md) | [да](../from/postgresql/ydb-importer.md) | [да](../from/greenplum/ydb-importer.md) | [да](../from/mysql/ydb-importer.md) | [да](../from/mssql/ydb-importer.md) | [да](../from/clickhouse/ydb-importer.md) | [да](../from/oracle/ydb-importer.md) |
| [Федеративные запросы](../tools/federated-queries.md) | [да](../from/postgresql/federated-queries.md) | [да](../from/greenplum/federated-queries.md) | [да](../from/mysql/federated-queries.md) | [да](../from/mssql/federated-queries.md) | [да](../from/clickhouse/federated-queries.md) | — |
| [dbt](../tools/dbt.md) | [да](../from/postgresql/dbt.md) | [да](../from/greenplum/dbt.md) | [да](../from/mysql/dbt.md) | [да](../from/mssql/dbt.md) | [да](../from/clickhouse/dbt.md) | — |
| [ydb-pg-extension](../tools/ydb-pg-extension.md) | [да](../from/postgresql/ydb-pg-extension.md) | — | — | — | — | — |
| [mysql2ydb](../tools/mysql2ydb.md) | — | — | [да](../from/mysql/mysql2ydb.md) | — | — | — |

