# STATISTICS

`STATISTICS` defines multi-column statistics for a new table. Run [ANALYZE](../analyze.md) to collect them.

The [cost-based optimizer](../../../../concepts/query_execution/optimizer.md) uses the resulting data to choose a query plan. By default, `ANALYZE` collects information about the table as a whole and about each column separately. Use `STATISTICS` to also analyze combinations of values across multiple columns.

Add `STATISTICS` to the table definition:

```yql
CREATE TABLE <table_name> (
    ...
    STATISTICS <statistics_name> ON ( <column_name> [, ...] )
        WITH ( <statistics_type> ),
    PRIMARY KEY ( ... )
);
```

[`SHOW CREATE TABLE`](../show_create.md) displays the multi-column statistics settings. To add or remove them for an existing table, use [`ALTER TABLE`](../alter_table/statistics.md).

## Parameters

* `table_name`: name of the table to create.
* `statistics_name`: name of the multi-column statistics.
* `column_name`: names of existing table columns. The column order matters.
* `statistics_type`: `COUNT_MIN_SKETCH`. Specify `WITH (COUNT_MIN_SKETCH)` explicitly: in this version, multi-column statistics are not collected if `WITH` is omitted.

## Statistics types

`COUNT_MIN_SKETCH` estimates how often a combination of values from multiple columns occurs. Specify at least two columns: a single column does not produce separate statistics.

For individual columns, `ANALYZE` builds sketches without `STATISTICS` when the estimated number of distinct values is less than 80% of the row count.

## Example

Configure a multi-column sketch when creating a table and start collecting statistics:

```yql
CREATE TABLE orders (
    customer_id Uint64,
    order_date Date,
    status Utf8,
    amount Int64,
    STATISTICS orders_cms ON (customer_id, order_date) WITH (COUNT_MIN_SKETCH),
    PRIMARY KEY (customer_id, order_date)
);

ANALYZE orders;
```
