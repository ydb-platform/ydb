# ANALYZE

`ANALYZE` collects statistics for the [{{ ydb-short-name }} cost-based optimizer](../../../concepts/query_execution/optimizer.md).

## Syntax

```yql
ANALYZE <path_to_table> [ (<column_name> [, ...]) ] [SAMPLE <rate>]
```

The statement runs synchronously and completes when the requested statistics have been collected and are up to date. Each run can process only one table.

* `path_to_table` — path of the table to collect statistics for.
* `column_name` — columns for which to collect statistics. If the list is omitted, `ANALYZE` processes every column in the table. A [multi-column statistic](create_table/statistics.md) is refreshed only when the list includes all of its columns. Statistics for the table as a whole are updated whether or not a column list is given.
* `SAMPLE <rate>` — collect statistics from a sample. `<rate>` is a numeric expression that must return a finite value of type `Double` in the range `(0, 1]`. `SAMPLE 1` reads the whole table, as does `ANALYZE` without `SAMPLE`.

The statistics collected are described in [{#T}](../../../concepts/query_execution/optimizer.md#statistics). Declare multi-column statistics with [STATISTICS](create_table/statistics.md) in `CREATE TABLE` or [ALTER TABLE](alter_table/statistics.md).

## What is collected

For each requested column, `ANALYZE` stores:

* the number of values and the number of distinct values;
* the minimum and the maximum, when the column type is numeric;
* a [count-min sketch](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch), when fewer than 80% of the values are distinct;
* an equi-width histogram, when the column is numeric and has more than one value.

Every `ANALYZE` collects statistics for the table as a whole and the per-column statistics listed above. To collect statistics over several columns together, declare the tuple with [STATISTICS](create_table/statistics.md) and include every column of that tuple in `ANALYZE`.

## Sampling

With a rate below `1`, the statistics are approximate.

* A row table is read in one pass. Each row is included in the sample with probability `<rate>` (Bernoulli sampling).
* For a column table, shards are picked at random. At least one shard is read, and sometimes all of them.

Sampled statistics are stored separately from the last full statistics. Background collection is scheduled based on the last full `ANALYZE`. The next full `ANALYZE`—without `SAMPLE` or with `SAMPLE 1`—replaces the sampled statistics. See [background collection](../../../concepts/query_execution/optimizer.md#statistics).

## Background operation

SQL operation `ANALYZE` executes synchronously and waits for completion. In addition a background operation is created for observability. Use the [operation list](../../../reference/ydb-cli/operation-list.md) command to monitor the progress of background `ANALYZE` operations.
