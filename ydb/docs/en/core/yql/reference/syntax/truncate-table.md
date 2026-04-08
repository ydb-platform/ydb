# TRUNCATE TABLE

<<<<<<< HEAD
`TRUNCATE TABLE` removes all user data from the specified table and its indexes.
=======
`TRUNCATE TABLE` deletes all user data from the specified table and its indexes.
>>>>>>> ecfe717e65e (DOCSUP-126759: [YDBDOCS-1922] Переводы Февраля - 12. Организация процесса перевода (1 архив) (0 шт.) (#37350))

## Syntax

```yql
TRUNCATE TABLE <table_name>;
```

## Limitations

* While the operation runs, the table is locked for reads and writes.
<<<<<<< HEAD
* The operation cannot be interrupted or canceled after it has started.
* The operation cannot be performed if the table has:
    - an [asynchronous secondary index](../../../concepts/query_execution/secondary_indexes.md#async),
    - a [changefeed](alter_table/changefeed.md),
    - [asynchronous replication](../../../concepts/async-replication.md).

## Examples

Removes all data from the table with the full path `/Root/test/my_table`.
=======
* The operation cannot be interrupted or rolled back after it has started.
* The operation cannot run if the table has:
    - An [asynchronous secondary index](../../../concepts/secondary_indexes.md#async),
    - A [changefeed](alter_table/changefeed.md),
    - [Asynchronous replication](../../../concepts/async-replication.md).

## Examples

Remove all data from a table with the full path `/Root/test/my_table`:
>>>>>>> ecfe717e65e (DOCSUP-126759: [YDBDOCS-1922] Переводы Февраля - 12. Организация процесса перевода (1 архив) (0 шт.) (#37350))

```yql
TRUNCATE TABLE `/Root/test/my_table`;
```

<<<<<<< HEAD
Removes all data from table `my_table` in the current database.
=======
Remove all data from table `my_table` in the current database:
>>>>>>> ecfe717e65e (DOCSUP-126759: [YDBDOCS-1922] Переводы Февраля - 12. Организация процесса перевода (1 архив) (0 шт.) (#37350))

```yql
TRUNCATE TABLE `my_table`;
```

## See also

<<<<<<< HEAD
* [CREATE TABLE](create_table/index.md)
* [ALTER TABLE](alter_table/index.md)
* [DROP TABLE](drop_table.md)
=======
* [CREATE TABLE](./create_table/index.md)
* [ALTER TABLE](./alter_table/index.md)
* [DROP TABLE](./drop_table.md)
>>>>>>> ecfe717e65e (DOCSUP-126759: [YDBDOCS-1922] Переводы Февраля - 12. Организация процесса перевода (1 архив) (0 шт.) (#37350))
