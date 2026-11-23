# statistics_config

The `statistics_config` section controls how column statistics are collected for the [cost-based optimizer](../../concepts/query_execution/optimizer.md#statistics).

## Configuration parameters

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `enable_background_column_stats_collection` | bool | `false` | Background statistics collection is enabled |

## Background collection {#enable-background-column-stats-collection}

`enable_background_column_stats_collection: true` enables automatic `ANALYZE` runs for user column tables. Background collection is disabled by default.

The next collection is scheduled 24 hours after the start of the previous `ANALYZE`. This interval is fixed and cannot be configured. If another table is being processed, collection waits for it to finish.

## Configuration example

Enable background statistics collection:

```yaml
statistics_config:
  enable_background_column_stats_collection: true
```

## See also

- [{#T}](../../concepts/query_execution/optimizer.md#statistics)
- [{#T}](../../yql/reference/syntax/analyze.md)
- [CREATE TABLE ... STATISTICS](../../yql/reference/syntax/create_table/statistics.md)
- [ALTER TABLE ... STATISTICS](../../yql/reference/syntax/alter_table/statistics.md)
