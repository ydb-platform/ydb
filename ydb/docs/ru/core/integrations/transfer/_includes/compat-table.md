# Совместимость инструментов переноса данных

В таблице перечислены способы переноса **данных** из реляционных СУБД в {{ ydb-short-name }}. Перенос схемы в scope статей не входит — предполагается, что целевые таблицы в {{ ydb-short-name }} уже созданы (или создаются инструментом вместе с данными, если это его штатное поведение).

| Инструмент ↓ / СУБД → | [PostgreSQL](../from/postgresql/index.md) | [MySQL / MariaDB](../from/mysql/index.md) | [Microsoft SQL Server](../from/mssql/index.md) | [ClickHouse](../from/clickhouse/index.md) | [Greenplum](../from/greenplum/index.md) | [Oracle Database](../from/oracle/index.md) | [IBM Db2](../from/db2/index.md) | [IBM Informix](../from/informix/index.md) | [SQLite](../from/sqlite/index.md) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| CLI import file | [да](../from/postgresql/cli-import-file.md) | [да](../from/mysql/cli-import-file.md) | [да](../from/mssql/cli-import-file.md) | [да](../from/clickhouse/cli-import-file.md) | [да](../from/greenplum/cli-import-file.md) | [да](../from/oracle/cli-import-file.md) | [да](../from/db2/cli-import-file.md) | [да](../from/informix/cli-import-file.md) | [да](../from/sqlite/cli-import-file.md) |
| Spark + ydb-spark-connector | [да](../from/postgresql/spark.md) | [да](../from/mysql/spark.md) | [да](../from/mssql/spark.md) | [да](../from/clickhouse/spark.md) | [да](../from/greenplum/spark.md) | [да](../from/oracle/spark.md) | [да](../from/db2/spark.md) | [да](../from/informix/spark.md) | [да](../from/sqlite/spark.md) |
| Федеративные запросы | [да](../from/postgresql/federated-queries.md) | [да](../from/mysql/federated-queries.md) | [да](../from/mssql/federated-queries.md) | [да](../from/clickhouse/federated-queries.md) | [да](../from/greenplum/federated-queries.md) | — | — | — | — |
| dbt (dbt-ydb) | [да](../from/postgresql/dbt.md) | [да](../from/mysql/dbt.md) | [да](../from/mssql/dbt.md) | [да](../from/clickhouse/dbt.md) | [да](../from/greenplum/dbt.md) | — | — | — | — |
| ydb-importer | [да](../from/postgresql/ydb-importer.md) | [да](../from/mysql/ydb-importer.md) | [да](../from/mssql/ydb-importer.md) | — | — | [да](../from/oracle/ydb-importer.md) | [да](../from/db2/ydb-importer.md) | — | — |
| pg_dump + pg-convert | [да](../from/postgresql/pg-dump.md) | — | — | — | — | — | — | — | — |
| ydb-pg-extension | [да](../from/postgresql/ydb-pg-extension.md) | — | — | — | — | — | — | — | — |
| mysql2ydb | — | [да](../from/mysql/mysql2ydb.md) | — | — | — | — | — | — | — |

{% note info %}

**dbt (dbt-ydb)** — материализация данных через External Data Source и `dbt run`; прямого подключения к исходной СУБД нет. Подробнее — в [инструкции по dbt](../../migration/dbt.md).

**SQLite + dbt** — только загрузка небольших справочников через [seeds](https://docs.getdbt.com/docs/build/seeds) из CSV; для полной миграции используйте [CLI import file](../from/sqlite/cli-import-file.md) или [Spark](../from/sqlite/spark.md).

{% endnote %}
