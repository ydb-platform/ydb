# Перенос данных из MySQL / MariaDB в {{ ydb-short-name }}

MySQL и MariaDB используют совместимый wire protocol и JDBC-драйверы.

Ниже — обзор доступных инструментов. Каждая ссылка ведёт на самодостаточную пошаговую инструкцию.

| Инструмент | Инструкция |
| --- | --- |
| CLI import file | [Инструкция](cli-import-file.md) |
| Spark + ydb-spark-connector | [Инструкция](spark.md) |
| Федеративные запросы | [Инструкция](federated-queries.md) |
| dbt (dbt-ydb) | [Инструкция](dbt.md) |
| ydb-importer | [Инструкция](ydb-importer.md) |
| mysql2ydb | [Инструкция](mysql2ydb.md) |

[← Матрица совместимости](../index.md)
