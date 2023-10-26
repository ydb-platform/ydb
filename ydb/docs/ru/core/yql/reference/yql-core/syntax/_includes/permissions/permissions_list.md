## Права доступа {#permissions-list}
В качестве имен прав доступа можно использовать специфичные для {{ ydb-short-name }} права или соответствующие им ключевые слова. В таблице ниже перечислены возможные имена прав и соответствующие им ключевые слова.
Нужно заметить, что специфичные для {{ ydb-short-name }} права задаются как строки и должны заключаться в одинарные или двойные кавычки.

{{ ydb-short-name }} право | Ключевое слово
---|---
`"ydb.database.connect"` | `CONNECT`
`"ydb.tables.modify"` | `MODIFY TABLES`
`"ydb.tables.read"` | `SELECT TABLES`
`"ydb.generic.list"` | `LIST`
`"ydb.generic.read"` | `SELECT`
`"ydb.generic.write"` | `INSERT`
`"ydb.access.grant"` | `GRANT`
`"ydb.generic.use"` | `USE`
`"ydb.generic.use_legacy"` | `USE LEGACY`
`"ydb.database.create"` | `CREATE`
`"ydb.database.drop"` | `DROP`
`"ydb.generic.manage"` | `MANAGE`
`"ydb.generic.full"` | `FULL`
`"ydb.generic.full_legacy"` | `FULL LEGACY`
`"ydb.granular.select_row"` | `SELECT ROW`
`"ydb.granular.update_row"` | `UPDATE ROW`
`"ydb.granular.erase_row"` | `ERASE ROW`
`"ydb.granular.read_attributes"` | `SELECT ATTRIBUTES`
`"ydb.granular.write_attributes"` | `MODIFY ATTRIBUTES`
`"ydb.granular.create_directory"` | `CREATE DIRECTORY`
`"ydb.granular.create_table"` | `CREATE TABLE`
`"ydb.granular.create_queue"` | `CREATE QUEUE`
`"ydb.granular.remove_schema"` | `REMOVE SCHEMA`
`"ydb.granular.describe_schema"` | `DESCRIBE SCHEMA`
`"ydb.granular.alter_schema"` | `ALTER SCHEMA`

* `ALL [PRIVILEGES]` - используется для указания всех возможных прав на объекты схемы для пользователей или групп. `PRIVILEGES` является необязательным ключевым словом, необходимым для совместимости с SQL стандартом.
