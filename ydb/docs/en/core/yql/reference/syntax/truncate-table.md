# TRUNCATE TABLE

`TRUNCATE TABLE` deletes all user data from the specified table and its indexes.

## Syntax

```yql
TRUNCATE TABLE <table_name>;
```

## Limitations

* While the operation runs, the table is locked for reads and writes.
* The operation cannot be interrupted or rolled back after it has started.
* The operation cannot run if the table has:
    - An [asynchronous secondary index](../../../concepts/query_execution/secondary_indexes.md#async),
    - A [changefeed](alter_table/changefeed.md),
    - [Asynchronous replication](../../../concepts/async-replication.md).

## Examples

Remove all data from a table with the full path `/Root/test/my_table`:

```yql
TRUNCATE TABLE `/Root/test/my_table`;
```

Remove all data from table `my_table` in the current database:

```yql
TRUNCATE TABLE `my_table`;
```

## See also

* [CREATE TABLE](./create_table/index.md)
* [ALTER TABLE](./alter_table/index.md)
* [DROP TABLE](./drop_table.md)
