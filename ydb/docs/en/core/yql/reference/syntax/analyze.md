# ANALYZE

`ANALYZE` collects statistics for a table and its columns. The [{{ ydb-short-name }} cost-based optimizer](../../../concepts/query_execution/optimizer.md) uses them to estimate how much data a query will process and choose an execution plan.

The statement works with one table and completes after updating the statistics.

## Syntax

```yql
ANALYZE <path_to_table> [ (<column_name> [, ...]) ] [SAMPLE <rate>]
```

Parameters:

* `path_to_table`: path to the table.
* `column_name`: columns to collect statistics for. If the list is omitted, all columns are processed.
* `SAMPLE <rate>`: sampled statistics collection. `rate` specifies the sampling rate and has type `Double`. The value must be in the interval `(0, 1]`. Without `SAMPLE`, or with `SAMPLE 1`, the entire table is read.

## Examples

Collect statistics for the `orders` table:

```yql
-- All columns
ANALYZE orders;

-- Selected columns
ANALYZE orders (customer_id, amount);

-- A sample with a rate of 0.1
ANALYZE orders SAMPLE 0.1;
```

## Collected statistics

`ANALYZE` collects statistics for selected columns: estimates of the number of distinct values, their frequencies, and their distribution. The statistics collected depend on the column's type and contents.

To collect multi-column statistics, add a `STATISTICS` clause to [CREATE TABLE](create_table/statistics.md) or [ALTER TABLE](alter_table/statistics.md). When running `ANALYZE`, include all columns of these statistics or omit the column list.

## Sampled statistics collection {#sampling}

To collect sampled statistics, add `SAMPLE` with a rate below `1`.

* For a row table, each row is independently included in the sample with probability `rate`.
* For a column table, shards are selected at random. At least one shard is read; when there are few shards, all of them may be read.

When sampling, the estimated number of distinct values is stored only if all shards of a column table are read.

Sampled statistics are stored separately from full statistics and replaced by the next full `ANALYZE`. Sampling does not affect the [background collection](../../../reference/configuration/statistics_config.md#enable-background-column-stats-collection) schedule, which is based on the last full `ANALYZE`.

## Monitoring progress

The SQL `ANALYZE` statement runs synchronously and waits for completion. A background operation is also created for observability. Use the [operation list](../../../reference/ydb-cli/operation-list.md) command to monitor the progress of background `ANALYZE` operations.
