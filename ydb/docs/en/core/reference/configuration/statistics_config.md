# statistics_config

The `statistics_config` section controls how column statistics are collected for the [cost-based optimizer](../../concepts/query_execution/optimizer.md#statistics).

## Configuration parameters

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| [`enable_background_column_stats_collection`](#enable-background-column-stats-collection) | bool | `false` | Enable background statistics collection. Disabled by default. |
| [`background_analyze_change_ratio_threshold_percent`](#background-analyze-change-ratio-threshold-percent) | uint32 | `20` | Change threshold for collection before the 24-hour interval expires, as a percentage |
| [`analyze_collect_primary_key_histogram`](#analyze-collect-primary-key-histogram) | bool | `false` | Build a primary-key histogram |
| [`analyze_column_table_whole_table_scan_max_bytes`](#analyze-column-table-whole-table-scan-max-bytes) | uint64 | `10737418240` (10 GiB) | Size threshold for reading a column table as a whole or shard by shard |
| [`analyze_row_table_whole_table_scan_max_bytes`](#analyze-row-table-whole-table-scan-max-bytes) | uint64 | `10737418240` (10 GiB) | Size threshold for reading a row table as a whole or in primary-key ranges |

## Background collection {#enable-background-column-stats-collection}

`enable_background_column_stats_collection: true` enables automatic `ANALYZE` runs for user tables. Background collection is disabled by default.

The next collection is scheduled 24 hours after the start of the previous full `ANALYZE`. This interval is fixed and cannot be configured. Collection may start earlier if the [change threshold](#background-analyze-change-ratio-threshold-percent) is reached. If another table is being processed, collection waits for it to finish.

Sampling (`ANALYZE SAMPLE` with a rate below `1`) does not change the schedule.

### Change threshold {#background-analyze-change-ratio-threshold-percent}

`background_analyze_change_ratio_threshold_percent` allows statistics to be refreshed after data changes without waiting for the scheduled 24-hour interval. It applies when background collection is enabled and sets a threshold as a percentage of the current row count. Row updates and deletes since the last full `ANALYZE` are counted toward the threshold.

For example, if the table currently has 1,000 rows, a 20% threshold corresponds to 200 updates or deletes. Repeated changes to the same row are counted separately.

## Primary-key histogram {#analyze-collect-primary-key-histogram}

When defining multi-column statistics with [CREATE TABLE ... STATISTICS](../../yql/reference/syntax/create_table/statistics.md), specify the columns to include.

To avoid listing the primary-key columns in `STATISTICS`, set `analyze_collect_primary_key_histogram: true`. This enables collection of a primary-key equi-height histogram during `ANALYZE`.

## Reading tables

A full `ANALYZE` reads every row in the table. The thresholds below determine whether reading is split by shard or primary-key range. Thresholds are specified in bytes.

### Column tables {#analyze-column-table-whole-table-scan-max-bytes}

`analyze_column_table_whole_table_scan_max_bytes` selects how a column table is read: as a whole or shard by shard. If the size is known and does not exceed the threshold, `ANALYZE` reads the whole table. Larger tables and tables of unknown size are scanned shard by shard.

A value of `0` always uses shard-by-shard scanning.

### Row tables {#analyze-row-table-whole-table-scan-max-bytes}

`analyze_row_table_whole_table_scan_max_bytes` selects how a row table is read: as a whole or by primary-key range. If the size is known and does not exceed the threshold, `ANALYZE` reads the whole table. Larger tables and tables of unknown size are scanned in primary-key ranges.

A value of `0` enables scanning by primary-key range. If the key does not allow the table to be split into ranges, the whole table is read.

## Configuration example

Enable background collection with a data change threshold of 20%:

```yaml
statistics_config:
  enable_background_column_stats_collection: true
  background_analyze_change_ratio_threshold_percent: 20
```

## See also

- [{#T}](../../concepts/query_execution/optimizer.md#statistics)
- [{#T}](../../yql/reference/syntax/analyze.md)
- [CREATE TABLE ... STATISTICS](../../yql/reference/syntax/create_table/statistics.md)
- [ALTER TABLE ... STATISTICS](../../yql/reference/syntax/alter_table/statistics.md)
