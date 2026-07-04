# Совместимость инструментов переноса данных

В таблице перечислены способы переноса **данных** из реляционных СУБД в {{ ydb-short-name }}. Перенос схемы в scope статей не входит — предполагается, что целевые таблицы в {{ ydb-short-name }} уже созданы (или создаются инструментом вместе с данными, если это его штатное поведение).

| Инструмент ↓ / СУБД → | [PostgreSQL](../from/postgresql.md) | [MySQL / MariaDB](../from/mysql.md) | [Microsoft SQL Server](../from/mssql.md) | [ClickHouse](../from/clickhouse.md) | [Greenplum](../from/greenplum.md) | [Oracle Database](../from/oracle.md) | [IBM Db2](../from/db2.md) | [IBM Informix](../from/informix.md) | [SQLite](../from/sqlite.md) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| [CLI import file](../from/postgresql.md#cli-import-file) | [да](../from/postgresql.md#cli-import-file) | [да](../from/mysql.md#cli-import-file) | [да](../from/mssql.md#cli-import-file) | [да](../from/clickhouse.md#cli-import-file) | [да](../from/greenplum.md#cli-import-file) | [да](../from/oracle.md#cli-import-file) | [да](../from/db2.md#cli-import-file) | [да](../from/informix.md#cli-import-file) | [да](../from/sqlite.md#cli-import-file) |
| [Spark + ydb-spark-connector](../from/postgresql.md#spark) | [да](../from/postgresql.md#spark) | [да](../from/mysql.md#spark) | [да](../from/mssql.md#spark) | [да](../from/clickhouse.md#spark) | [да](../from/greenplum.md#spark) | [да](../from/oracle.md#spark) | [да](../from/db2.md#spark) | [да](../from/informix.md#spark) | [да](../from/sqlite.md#spark) |
| [Федеративные запросы](../from/postgresql.md#federated-queries) | [да](../from/postgresql.md#federated-queries) | [да](../from/mysql.md#federated-queries) | [да](../from/mssql.md#federated-queries) | [да](../from/clickhouse.md#federated-queries) | [да](../from/greenplum.md#federated-queries) | — | — | — | — |
| [dbt (dbt-ydb)](../from/postgresql.md#dbt) | [да](../from/postgresql.md#dbt) | [да](../from/mysql.md#dbt) | [да](../from/mssql.md#dbt) | [да](../from/clickhouse.md#dbt) | [да](../from/greenplum.md#dbt) | — | — | — | — |
| [ydb-importer](../from/postgresql.md#ydb-importer) | [да](../from/postgresql.md#ydb-importer) | [да](../from/mysql.md#ydb-importer) | [да](../from/mssql.md#ydb-importer) | — | — | [да](../from/oracle.md#ydb-importer) | [да](../from/db2.md#ydb-importer) | — | — |
| [pg_dump + pg-convert](../from/postgresql.md#pg-dump) | [да](../from/postgresql.md#pg-dump) | — | — | — | — | — | — | — | — |
| [ydb-pg-extension](../from/postgresql.md#ydb-pg-extension) | [да](../from/postgresql.md#ydb-pg-extension) | — | — | — | — | — | — | — | — |
| [mysql2ydb](../from/mysql.md#mysql2ydb) | — | [да](../from/mysql.md#mysql2ydb) | — | — | — | — | — | — | — |

{% note info %}

**dbt (dbt-ydb)** — материализация данных через External Data Source и `dbt run`; прямого подключения к исходной СУБД нет. Подробнее — в [инструкции по dbt](../migration/dbt.md).

**SQLite + dbt** — только загрузка небольших справочников через [seeds](https://docs.getdbt.com/docs/build/seeds) из CSV; для полной миграции используйте CLI import file или Spark.

{% endnote %}
