## Права доступа {#permissions-list}

В качестве имён прав доступа можно использовать имена {{ ydb-short-name }} прав или соответствующие им ключевые слова YQL.

[//]: # (TODO: не получается поставить ссылку на concepts/glossary.md#access-right)

{{ ydb-short-name }} право | Ключевое слово YQL | Описание
--- | --- | ---
*Права уровня кластера*
`ydb.database.create` | `CREATE` | Право создавать новые базы данных в кластере
`ydb.database.drop` | `DROP` | Право удалять базы данных в кластере
*Права уровня базы данных*
`ydb.database.connect` | `CONNECT` | Право подключаться к базе данных
*Элементарные права уровня объектов схемы*
`ydb.granular.select_row` | `SELECT ROW` | Право читать строки из таблицы (select), читать сообщения сообщения из топиков
`ydb.granular.update_row` | `UPDATE ROW` | Право обновлять строки в таблице (insert, update, upsert, replace), писать сообщения в топики
`ydb.granular.erase_row` | `ERASE ROW` | Право удалять строки из таблицы (delete)
`ydb.granular.create_directory` | `CREATE DIRECTORY` | Право создавать и удалять директории, в том числе существующие и вложенные
`ydb.granular.create_table` | `CREATE TABLE` | Право создавать таблицы (в том числе индексные, внешние, колоночные), представления, последовательности
`ydb.granular.create_queue` | `CREATE QUEUE` | Право создавать топики
`ydb.granular.remove_schema` | `REMOVE SCHEMA` | Право удалять объекты (директории, таблицы, топики), которые были созданы посредством использования прав
`ydb.granular.describe_schema` | `DESCRIBE SCHEMA` | Право просмотра имеющихся прав доступа (ACL) на объект доступа, просмотра описания объектов доступа (директории, таблицы, топики)
`ydb.granular.alter_schema` | `ALTER SCHEMA` | Право изменять объекты доступа (директории, таблицы, топики), в том числе права пользователей на объекты доступа
*Право передачи прав*
`ydb.access.grant` | `GRANT` | Флаг, помечающий другие выдаваемые права, что эти права пользователь может также выдать другим пользователям (аналог `WITH GRANT OPTION`)
*Группы прав*
`ydb.tables.modify` | `MODIFY TABLES` | `ydb.granular.update_row` + `ydb.granular.erase_row`
`ydb.tables.read` | `SELECT TABLES` | Синоним `ydb.granular.select_row`
`ydb.generic.list` | `LIST` | Синоним `ydb.granular.describe_schema`
`ydb.generic.read` | `SELECT` | `ydb.granular.select_row` + `ydb.generic.list`
`ydb.generic.write` | `INSERT` | `ydb.granular.update_row` + `ydb.granular.erase_row` + `ydb.granular.create_directory` + `ydb.granular.create_table` + `ydb.granular.create_queue` + `ydb.granular.remove_schema` + `ydb.granular.alter_schema`
`ydb.generic.use_legacy` | `USE LEGACY` | `ydb.generic.read` + `ydb.generic.write` + `ydb.access.grant`
`ydb.generic.use` | `USE` | `ydb.generic.use_legacy` + `ydb.database.connect`
`ydb.generic.manage` | `MANAGE` | `ydb.database.create` + `ydb.database.drop`
`ydb.generic.full_legacy` | `FULL LEGACY` | `ydb.generic.use_legacy` + `ydb.generic.manage`
`ydb.generic.full` | `FULL` | `ydb.generic.use` + `ydb.generic.manage`

* `ALL [PRIVILEGES]` - используется для указания всех возможных прав на объекты схемы для пользователей или групп. `PRIVILEGES` является необязательным ключевым словом, необходимым для совместимости с SQL стандартом.

{% note info %}

Права доступа следует рассматривать как слои дополнительных разрешений: для изменения объекта помимо права на изменение требуются права на доступ к базе и на чтение.

{% endnote %}

```mermaid
---
config:
  flowchart:
  # guard long node labels from wrapping or clipping
  - wrappingWidth: 500  # default is 200
---
graph BT
    ydb.database.connect --> ydb.granular.describe_schema

    ydb.granular.describe_schema --> ydb.granular.select_row & create_schema & ydb.granular.remove_schema & ydb.granular.alter_schema

    ydb.granular.select_row --> ydb.granular.update_row & ydb.granular.erase_row

    ydb.database.connect["`ydb.database.connect`"]
    ydb.granular.describe_schema["`ydb.granular.describe_schema`"]
    create_schema["`ydb.granular.create_{directory,table,queue}`"]
    ydb.granular.remove_schema["`ydb.granular.remove_schema`"]
    ydb.granular.alter_schema["`ydb.granular.alter_schema`"]
    ydb.granular.select_row["`ydb.granular.select_row`"]
    ydb.granular.update_row["`ydb.granular.update_row`"]
    ydb.granular.erase_row["`ydb.granular.erase_row`"]
```

Например:

1. для изменения данных в таблицах нужны права:

    + `ydb.granular.update_row`
    + `ydb.granular.select_row`
    + `ydb.granular.describe_schema`
    + `ydb.database.connect`

2. для изменения объекта схемы нужны права:

    + `ydb.granular.alter_schema`
    + `ydb.granular.describe_schema`
    + `ydb.database.connect`
