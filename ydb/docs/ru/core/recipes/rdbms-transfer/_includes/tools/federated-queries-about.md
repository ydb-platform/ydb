## Федеративные запросы {#about}

**Федеративные запросы** позволяют узлам {{ ydb-short-name }} читать данные из внешней СУБД через [External Data Source](../../../../concepts/datamodel/external_data_source.md) и записывать в локальную таблицу одним YQL (`UPSERT INTO … SELECT …`).

### Когда использовать

* Источник доступен по сети **с узлов кластера** {{ ydb-short-name }}.
* Нужен SQL-пайплайн без промежуточных файлов и без Spark.
* Поддерживается ваш тип источника (PostgreSQL, MySQL, MSSQL, ClickHouse, Greenplum).

### Системные требования

| Компонент | Требование |
| --- | --- |
| Кластер {{ ydb-short-name }} | Feature flag `enable_external_data_sources` |
| Сеть | Источник доступен с **каждого** узла {{ ydb-short-name }}, не только с рабочей станции |
| YQL | Права на `CREATE SECRET`, `CREATE EXTERNAL DATA SOURCE`, DML |

{% note warning %}

Коннекторы к внешним СУБД находятся в стадии Preview. Перед production проверьте [ограничения](../../../../concepts/query_execution/federated_query/index.md) для вашего источника.

{% endnote %}

Подробнее: [импорт через федеративные запросы](../../../../concepts/query_execution/federated_query/import_and_export.md).
