# STATISTICS

`ADD STATISTICS` defines multi-column statistics for an existing table. Run [ANALYZE](../analyze.md) to collect them.

`DROP STATISTICS` removes the multi-column statistics with the specified name.

Statistics types and column requirements are described in [CREATE TABLE ... STATISTICS](../create_table/statistics.md). An `ALTER TABLE` statement can include several of these actions along with other table changes.

## Examples

Add two equi-height histograms and run `ANALYZE`:

```yql
ALTER TABLE orders
    ADD STATISTICS amount_category ON (amount, category) WITH (EQ_HEIGHT_HISTOGRAM),
    ADD STATISTICS amount_time ON (amount, created_at) WITH (EQ_HEIGHT_HISTOGRAM);

ANALYZE orders;
```

Remove the multi-column statistics named `amount_category`:

```yql
ALTER TABLE orders DROP STATISTICS amount_category;
```
