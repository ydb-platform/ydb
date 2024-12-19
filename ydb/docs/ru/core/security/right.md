# Права

## Основные понятия

Обладание правом в {{ ydb-short-name }} даёт возможность выполнять определённые операции в кластере или базе данных.
Права в {{ ydb-short-name }} выдаются на объекты доступа. У каждого такого объекта есть список разрешений — ACL (Access Control List) — он хранит все предоставленные пользователям и группам права на объект. Объектом может быть директория, содержащая другие поддиректории или непосредственно объекты, не имеющие других вложенных объектов, — таблицы, топики, представления и не только.
В дальнейшем, если внутри директории появляются другие объекты, они по умолчанию наследуют все права, выданные на родительскую директорию.

{% note info %}

Права привязаны не пользователю, а к объекту доступа.

{% endnote %}

## Перечень прав

В качестве имён прав доступа можно использовать имена {{ ydb-short-name }} прав или соответствующие им ключевые слова YQL.
В таблице ниже перечислены возможные имена прав.

{{ ydb-short-name }} право | Ключевое слово YQL | Описание
--- | --- | ---
Права уровня баз данных
`ydb.database.connect` | `CONNECT` | Право подключаться к базе данных
`ydb.database.create` | `CREATE` | Право создавать новые базы данных в кластере
`ydb.database.drop` | `DROP` | Право удалять базы данных в кластере
Элементарные права на объекты базы данных
`ydb.granular.select_row` | `SELECT ROW` | Право читать строки из таблицы (select), читать сообщения сообщения из топиков
`ydb.granular.update_row` | `UPDATE ROW` | Право обновлять строки в таблице (insert, update, insert, erase), писать сообщения в топики
`ydb.granular.erase_row` | `ERASE ROW` | Право удалять строки из таблицы
`ydb.granular.create_directory` | `CREATE DIRECTORY` | Право создавать и удалять директории, в том числе существующие и вложенные.
`ydb.granular.create_table` | `CREATE TABLE` | Право создавать таблицы (в том числе индексные, внешние, колоночные), представления, последовательности.
`ydb.granular.create_queue` | `CREATE QUEUE` | Право создавать топики
`ydb.granular.remove_schema` | `REMOVE SCHEMA` | Право удалять объекты (директории, таблицы, топики), которые были созданы посредством использования прав
`ydb.granular.describe_schema` | `DESCRIBE SCHEMA` | Право просмотра имеющихся прав доступа (ACL) на объект доступа, просмотра описания объектов доступа (директории, таблицы, топики)
`ydb.granular.alter_schema` | `ALTER SCHEMA` | Право изменять объекты доступа (директории, таблицы, топики), в том числе права пользователей на объекты доступа
Дополнительные флаги
`ydb.access.grant` | `GRANT` | Дополнительный флаг, помечающий другие выдаваемые права, что их пользователь может также выдать другим пользователям (аналог `WITH GRANT OPTION`)
Права, основанные на других правах
`ydb.tables.modify` | `MODIFY TABLES` | `ydb.granular.update_row` + `ydb.granular.erase_row`
`ydb.tables.read` | `SELECT TABLES` | Синоним `ydb.granular.select_row`
`ydb.generic.list` | `LIST` | Синоним `ydb.granular..describe_schema`
`ydb.generic.read` | `SELECT` | `ydb.granular.select_row` + `ydb.generic.list`
`ydb.generic.write` | `INSERT` | `ydb.granular.update_row` + `ydb.granular.erase_row` + `ydb.granular.create_directory` + `ydb.granular.create_table` + `ydb.granular.create_queue` + `ydb.granular.remove_schema` + `ydb.granular.alter_schema`
`ydb.generic.use_legacy` | `USE LEGACY` | `ydb.generic.read` + `ydb.generic.write` + `ydb.access.grant`
`ydb.generic.use` | `USE` | `ydb.generic.use_legacy` + `ydb.database.connect`
`ydb.generic.manage` | `MANAGE` | `ydb.database.create` + `ydb.database.drop`
`ydb.generic.full_legacy` | `FULL LEGACY` | `ydb.generic.use_legacy` + `ydb.generic.manage`
`ydb.generic.full` | `FULL` | `ydb.generic.use` + `ydb.generic.manage`

{% note info %}

Права `ydb.database.connect`, `ydb.granular.describe_schema`, `ydb.granular.select_row`, `ydb.granular.update_row` необходимо рассматривать как слои прав.
Например, для изменения строк необходимо не только право `ydb.granular.update_row`, но и все предыдущие права.

{% endnote %}

## Управление правами

### Управление правами с помощью YQL

Для управления правами служат следующие YQL-команды:

* [{#T}](../yql/reference/syntax/grant.md).
* [{#T}](../yql/reference/syntax/revoke.md).

Это команды принимают названия прав в обоих стилях: "{{ ydb-short-name }} право.2 и "Ключевое слово YQL".

Пример использования c "Ключевое слово YQL":

```yql
GRANT CREATE DIRECTORY ON `/Root/db1` TO testuser
```

Пример использования c "{{ ydb-short-name }} право":

```yql
GRANT "ydb.granular.create_directory" ON `/Root/db1` TO testuser
```

### Управление правами с помощью CLI

Для управления правами служат следующие CLI-команды:

* [{#T}]((../reference/ydb-cli/commands/scheme-permissions.md#grant)).
* [{#T}]((../reference/ydb-cli/commands/scheme-permissions.md#revoke)).

Через CLI используется только стиль из столбца "{{ ydb-short-name }} право".
Например:

```bash
ydb scheme permissions grant -p "ydb.granular.create_directory" `/Root/db1` testuser
```
