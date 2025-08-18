# Renaming a table

```yql
ALTER TABLE old_table_name RENAME TO new_table_name;
```

{% if oss == true and backend_name == "YDB" %}

{% cut "See table and column naming rules" %}

{% include [table naming rules](../../../../concepts/datamodel/_includes/object-naming-rules.md) %}

{% endcut %}

{% endif %}

If a table with the new name already exists, an error will be returned. The ability to transactionally replace a table under load is supported by specialized methods in CLI and SDK.

{% note warning %}

If a YQL query contains multiple `ALTER TABLE ... RENAME TO ...` commands, each will be executed in auto-commit mode in a separate transaction. From the perspective of an external process, the tables will be renamed sequentially, one after another. To rename multiple tables in a single transaction, use specialized methods available in CLI and SDK.

{% endnote %}

Renaming can be used to move a table from one directory within the database to another, for example:

``` yql
ALTER TABLE `table1` RENAME TO `/backup/table1`;
```
