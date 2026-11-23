# ANALYZE

`ANALYZE` collects statistics for a table and its columns. The [{{ ydb-short-name }} cost-based optimizer](../../../concepts/query_execution/optimizer.md) uses them to estimate how much data a query will process and choose an execution plan.

The statement works with one table and completes after updating the statistics.

## Syntax

```yql
ANALYZE <path_to_table> [ (<column_name> [, ...]) ]
```

Parameters:

* `path_to_table`: path to the table.
* `column_name`: columns to collect statistics for. If the list is omitted, all columns are processed.

## Examples

Collect statistics for the `orders` table:

```yql
-- All columns
ANALYZE orders;

-- Selected columns
ANALYZE orders (customer_id, amount);
```

## Collected statistics

`ANALYZE` collects statistics for selected columns: estimates of the number of distinct values, their frequencies, and their distribution. The statistics collected depend on the column's type and contents.

To collect multi-column statistics, add a `STATISTICS` clause to [CREATE TABLE](create_table/statistics.md) or [ALTER TABLE](alter_table/statistics.md). When running `ANALYZE`, include all columns of these statistics or omit the column list.

## Monitoring progress

The SQL `ANALYZE` statement runs synchronously and waits for completion. A background operation is also created for observability. Use the [operation list](../../../reference/ydb-cli/operation-list.md) command to monitor the progress of background `ANALYZE` operations.
