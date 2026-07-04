# Перенос данных из PostgreSQL в {{ ydb-short-name }}

PostgreSQL — один из наиболее поддерживаемых источников для переноса данных в {{ ydb-short-name }}.

Ниже — обзор доступных инструментов. Каждая ссылка ведёт на самодостаточную пошаговую инструкцию.

| Инструмент | Инструкция |
| --- | --- |
| CLI import file | [Инструкция](cli-import-file.md) |
| Spark + ydb-spark-connector | [Инструкция](spark.md) |
| Федеративные запросы | [Инструкция](federated-queries.md) |
| dbt (dbt-ydb) | [Инструкция](dbt.md) |
| ydb-importer | [Инструкция](ydb-importer.md) |
| pg_dump + pg-convert | [Инструкция](pg-dump.md) |
| ydb-pg-extension | [Инструкция](ydb-pg-extension.md) |

[← Матрица совместимости](../index.md)
