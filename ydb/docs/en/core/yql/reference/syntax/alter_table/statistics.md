# STATISTICS

`ADD STATISTICS` defines multi-column statistics for an existing table. Run [ANALYZE](../analyze.md) to collect them.

`DROP STATISTICS` removes only the collection settings for the specified statistics name.

Statistics types and column requirements are described in [{#T}](../create_table/statistics.md). An `ALTER TABLE` statement can include several of these actions along with other table changes.

## Examples

Add two equi-height histograms and run `ANALYZE`:

```yql
ALTER TABLE orders
    ADD STATISTICS amount_category ON (amount, category) WITH (EQ_HEIGHT_HISTOGRAM),
    ADD STATISTICS amount_time ON (amount, created_at) WITH (EQ_HEIGHT_HISTOGRAM);

ANALYZE orders;
```

Remove the collection settings for `amount_category`:

```yql
ALTER TABLE orders DROP STATISTICS amount_category;
```
