# STATISTICS

```yql
ALTER TABLE <table_name> ADD STATISTICS <statistics_name>
    ON ( <column_name> [, ...] )
    [WITH ( <statistics_type> [, ...] )];

ALTER TABLE <table_name> DROP STATISTICS <statistics_name>;
```

`ADD STATISTICS` adds a declaration of multi-column statistics to an existing table. `DROP STATISTICS` removes that declaration. Supported types and column rules are described in [{#T}](../create_table/statistics.md). To collect the declared statistics, run [ANALYZE](../analyze.md).

An `ALTER TABLE` statement can include several of these actions along with other table changes.

## Examples

Add two equi-height histograms:

```yql
ALTER TABLE orders
    ADD STATISTICS amount_category ON (amount, category) WITH (EQ_HEIGHT_HISTOGRAM),
    ADD STATISTICS amount_time ON (amount, created_at) WITH (EQ_HEIGHT_HISTOGRAM);
```

Remove a declaration:

```yql
ALTER TABLE orders DROP STATISTICS amount_category;
```
