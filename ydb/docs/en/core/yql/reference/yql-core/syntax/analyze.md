# ANALYZE

`ANALYZE` forces the collection of statistics for {{ ydb-short-name }} Cost Based Optimizer.

Syntax:

```yql
ANALYZE <path_to_table> [ (<column_name> [, ...]) ]
```

This command forces synchronous collection of table statistics and column statistics for the specied columns, or all columns of the table if the columns were not specified. ANALYZE will return when all the specified statistics has been collect and is up to date.

* `path_to_table` — path to the table for which the statistis should be collected
* `column_name` — collect column statistics only for specified columns of the table

Current set of statistics is described here [{#T}](../../../concepts/optimizer.md#statistics).
