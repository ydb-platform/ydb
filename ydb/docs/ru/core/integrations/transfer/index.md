# Перенос данных в {{ ydb-short-name }}

В этом разделе собран обзор способов переноса **данных** из реляционных СУБД в {{ ydb-short-name }} и пошаговые инструкции для каждого источника.

{% include notitle [Совместимость инструментов](_includes/compat-table.md) %}

## Инструкции по источникам

| СУБД-источник | Обзор |
| --- | --- |
| PostgreSQL | [Перенос из PostgreSQL](from/postgresql/index.md) |
| MySQL / MariaDB | [Перенос из MySQL / MariaDB](from/mysql/index.md) |
| Microsoft SQL Server | [Перенос из Microsoft SQL Server](from/mssql/index.md) |
| ClickHouse | [Перенос из ClickHouse](from/clickhouse/index.md) |
| Greenplum | [Перенос из Greenplum](from/greenplum/index.md) |
| Oracle Database | [Перенос из Oracle Database](from/oracle/index.md) |
| IBM Db2 | [Перенос из IBM Db2](from/db2/index.md) |
| IBM Informix | [Перенос из IBM Informix](from/informix/index.md) |
| SQLite | [Перенос из SQLite](from/sqlite/index.md) |

## Смотрите также

* [Импорт из JDBC](../data-migration/import-jdbc.md) — подробности про ydb-importer
* [Импорт из MySQL](../data-migration/import-mysql.md) — подробности про mysql2ydb
* [Импорт данных из PostgreSQL](../../postgresql/import.md) — pg_dump и pg-convert
* [Федеративные запросы: импорт и экспорт](../../concepts/query_execution/federated_query/import_and_export.md)
* [{{ spark-name }}](../query-engines/spark.md) — YDB Spark Connector
* [{{ dbt }}](../migration/dbt.md) — dbt-ydb
