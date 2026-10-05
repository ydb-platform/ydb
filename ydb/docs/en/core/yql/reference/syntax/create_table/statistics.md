# STATISTICS

`STATISTICS` declares statistics over a tuple of columns. [ANALYZE](../analyze.md) collects them for the [cost-based optimizer](../../../../concepts/query_execution/optimizer.md).

Without `STATISTICS`, `ANALYZE` already collects table statistics and per-column statistics. Use `STATISTICS` to additionally collect statistics over combinations of columns.

```yql
CREATE TABLE <table_name> (
    ...
    STATISTICS <statistics_name> ON ( <column_name> [, ...] )
        [WITH ( <statistics_type> [, ...] )],
    PRIMARY KEY ( ... )
);
```

`SHOW CREATE TABLE` displays these declarations. To add or remove one on an existing table, use [`ALTER TABLE`](../alter_table/statistics.md).

## Parameters

* `statistics_name` — name of the declaration.
* `column_name` — columns in the tuple. Each name must match a column in the table, and the column order matters.
* `statistics_type` — `COUNT_MIN_SKETCH` or `EQ_HEIGHT_HISTOGRAM`. Without `WITH`, every supported type is requested.

`COUNT_MIN_SKETCH` is useful only for two or more columns. For a single column, the declaration does not create a separate statistic. A per-column sketch is built when fewer than 80% of the values are distinct.

`EQ_HEIGHT_HISTOGRAM` can be declared only for types whose values have a total order. `Json`, `Yson`, and `JsonDocument` do not, so declaring this histogram for these types results in an error.

`ANALYZE` collects a declared equi-height histogram even when automatic primary-key histogram collection is disabled. See [`analyze_collect_primary_key_histogram`](../../../../reference/configuration/statistics_config.md#analyze-collect-primary-key-histogram).

`DROP STATISTICS` removes only the declaration. To collect statistics, run [ANALYZE](../analyze.md).

## Example

```yql
CREATE TABLE orders (
    customer_id Uint64,
    order_date Date,
    status Utf8,
    amount Int64,
    PRIMARY KEY (customer_id, order_date),
    STATISTICS orders_hist ON (customer_id, order_date) WITH (EQ_HEIGHT_HISTOGRAM),
    STATISTICS status_hist ON (status) WITH (EQ_HEIGHT_HISTOGRAM)
);
```
