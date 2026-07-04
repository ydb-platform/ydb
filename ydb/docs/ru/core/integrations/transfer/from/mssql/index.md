# Перенос данных из Microsoft SQL Server в {{ ydb-short-name }}

Microsoft SQL Server поддерживается через JDBC, федеративные запросы и ydb-importer.

Ниже — обзор доступных инструментов. Каждая ссылка ведёт на самодостаточную пошаговую инструкцию.

| Инструмент | Инструкция |
| --- | --- |
| CLI import file | [Инструкция](cli-import-file.md) |
| Spark + ydb-spark-connector | [Инструкция](spark.md) |
| Федеративные запросы | [Инструкция](federated-queries.md) |
| dbt (dbt-ydb) | [Инструкция](dbt.md) |
| ydb-importer | [Инструкция](ydb-importer.md) |

[← Матрица совместимости](../index.md)
