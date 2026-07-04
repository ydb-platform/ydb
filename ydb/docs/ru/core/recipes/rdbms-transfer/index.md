# Перенос данных из других СУБД в {{ ydb-short-name }}

Рецепты переноса **данных** из реляционных СУБД в {{ ydb-short-name }}: матрица инструментов и пошаговые инструкции для каждой пары «источник + инструмент».

{% include notitle [Совместимость инструментов](_includes/compat-table.md) %}

## Инструменты

| Инструмент | Описание |
| --- | --- |
| [CLI import file](tools/cli-import-file.md) | Загрузка CSV/JSON/Parquet через `ydb import file` |
| [Spark](tools/spark.md) | JDBC/файлы → {{ ydb-short-name }} через Spark и YDB Connector |
| [Федеративные запросы](tools/federated-queries.md) | `UPSERT … SELECT` из External Data Source |
| [dbt](tools/dbt.md) | Материализация SQL-моделей (dbt-ydb) |
| [ydb-importer](tools/ydb-importer.md) | Параллельный JDBC-импорт |
| [ydb-pg-extension](tools/ydb-pg-extension.md) | Миграция изнутри PostgreSQL |
| [mysql2ydb](tools/mysql2ydb.md) | Специализированный перенос MySQL |

## Источники данных

| СУБД | Обзор рецептов |
| --- | --- |
| PostgreSQL | [from/postgresql/](from/postgresql/index.md) |
| MySQL / MariaDB | [from/mysql/](from/mysql/index.md) |
| Microsoft SQL Server | [from/mssql/](from/mssql/index.md) |
| ClickHouse | [ClickHouse](from/clickhouse/index.md) |
| Greenplum | [from/greenplum/](from/greenplum/index.md) |
| Oracle Database | [from/oracle/](from/oracle/index.md) |
| IBM Db2 | [from/db2/](from/db2/index.md) |
| IBM Informix | [from/informix/](from/informix/index.md) |
| SQLite | [from/sqlite/](from/sqlite/index.md) |

## Смотрите также

* [Импорт из JDBC](../../integrations/data-migration/import-jdbc.md)
* [Импорт из MySQL](../../integrations/data-migration/import-mysql.md)
* [Федеративные запросы: импорт и экспорт](../../concepts/query_execution/federated_query/import_and_export.md)
* [Spark](../../integrations/query-engines/spark.md)
* [dbt](../../integrations/migration/dbt.md)
