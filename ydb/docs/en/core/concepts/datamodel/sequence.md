# Sequence

A Sequence is an integer value generator owned by a column in a [row-oriented table](table.md#row-oriented-tables) in {{ ydb-short-name }}. {{ ydb-short-name }} creates it automatically for every [serial column](../../yql/reference/types/serial.md), including key and non-key columns, to generate values when a row does not provide one.

## Ownership and lifecycle

The generated object is private and located at `<table_path>/_serial_column_<column_name>`, where `<table_path>` is the table path and `<column_name>` is the serial column name. For example, the `user_id` column of `/Root/users` has its generator at `/Root/users/_serial_column_user_id`. The object is hidden from schema object listings, and its lifecycle is tied to the column.

You can change the generator parameters using [ALTER SEQUENCE](../../yql/reference/syntax/alter-sequence.md) at this path. YQL does not support standalone `CREATE SEQUENCE` or `DROP SEQUENCE` statements because the object is created and dropped with its serial column.

Dropping a non-key Serial column drops its Sequence. Dropping the table also drops all Sequences that it owns.

## Generated values

By default, the generator starts at 1, increments by 1, and has bounds determined by the serial type. The [serial type reference](../../yql/reference/types/serial.md) describes the available types and their bounds.

{{ ydb-short-name }} allocates a value before writing the table row. A value remains consumed even if the write fails or its transaction is rolled back, so gaps are allowed. Concurrent writes can also receive values in an order different from the order in which their rows are created.

Resetting with `ALTER SEQUENCE RESTART` can reuse values. A serial type does not by itself impose a uniqueness constraint on a column. For details on explicit values and conflicts on the full primary key, see [{#T}](../../yql/reference/types/serial.md#explicit-values).

## Table operations

Tables with Serial columns support the following operations:

- [copy](../../reference/ydb-cli/tools-copy.md)
- [rename](../../reference/ydb-cli/commands/tools/rename.md)
- [dump](../../reference/ydb-cli/export-import/tools-dump.md) and [restore](../../reference/ydb-cli/export-import/import-file.md)
- [S3 import](../../reference/ydb-cli/export-import/import-s3.md) and [S3 export](../../reference/ydb-cli/export-import/export-s3.md)

When a table is renamed, the paths of its sequences change to reflect the new table path, while their parameters and current positions are preserved.

When dumping such a table, do not use [`--avoid-copy`](../../reference/ydb-cli/export-import/tools-dump.md), because without copying, the dump does not save the current generator position.
