# Setting and dropping the `NOT NULL` constraint

A column-level data integrity constraint that prohibits writing `NULL` as values. This constraint ensures that the column always contains a valid value.

In YDB, the `SET NOT NULL` operation is performed as a synchronous SQL operation that waits for the schema change to be applied. At the same time, a background operation is created to check the table for `NULL` values in existing data.

## Setting `NOT NULL`

`SET NOT NULL` sets the `NOT NULL` constraint for the specified column.

For example, the following query will set the `NOT NULL` constraint for column `column_name` in table `table_name`:


```yql
ALTER TABLE table_name ALTER COLUMN column_name SET NOT NULL;
```


Notes:

* `SET NOT NULL` is only supported for [row tables](../../../../concepts/datamodel/table.md#row-oriented-tables).
* `SET NOT NULL` may take a long time: before setting the constraint, YDB checks the table for `NULL` values in the existing data of the specified column.
* The SQL operation is performed synchronously and waits for completion. At the same time, a background operation is created for observability.
* You can monitor the progress of operations using the [CLI command](../../../../reference/ydb-cli/operation-list.md) `ydb operation list setnotnull`. There are also commands to [get the status of a specific operation](../../../../reference/ydb-cli/operation-get.md), [cancel an operation](../../../../reference/ydb-cli/operation-cancel.md), or [delete the record of a completed operation](../../../../reference/ydb-cli/operation-forget.md).
* After starting the `SET NOT NULL` operation and before its completion, you cannot write `NULL` values to the specified column. If you try to write such values, you will get an error like:

  ```text
  Can't set NULL or optional value to column: <column>.
  SET NOT NULL operation is currently in progress for this column
  ```

* If validation fails, the `SET NOT NULL` operation will complete with error `Validation failed for SET NOT NULL on table ...: one or more columns contain NULL values`.

## Dropping `NOT NULL`

`DROP NOT NULL` removes the `NOT NULL` constraint from the specified column.

For example, the following query will remove the `NOT NULL` constraint from column `column_name` in table `table_name`:


```yql
ALTER TABLE table_name ALTER COLUMN column_name DROP NOT NULL;
```


Note:

* `DROP NOT NULL` is only supported for [row tables](../../../../concepts/datamodel/table.md#row-oriented-tables).

## See also

* [ALTER COLUMN](columns.md)
