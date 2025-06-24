# ANALYZE

`ANALYZE` forces the collection of statistics for [{{ ydb-short-name }} cost-based optimizer](../../../concepts/optimizer.md).

## Syntax

```yql
ANALYZE <path_to_table> [ (<column_name> [, ...]) ]
```

This command forces the synchronous collection of table statistics and column statistics for the specified columns or for all columns if none are specified. `ANALYZE` returns once all the requested statistics have been collected and are up to date.

* `path_to_table` — the path to the table for which statistics should be collected.
* `column_name` — collect column statistics only for the specified columns of the table.

The current set of statistics is described in [{#T}](../../../concepts/optimizer.md#statistics).
