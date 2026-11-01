# statistics_config

The `statistics_config` section controls how column statistics are collected for the [cost-based optimizer](../../concepts/query_execution/optimizer.md#statistics).

Run [ANALYZE](../../yql/reference/syntax/analyze.md) to collect statistics on demand. To collect multi-column statistics, list the columns in a [STATISTICS](../../yql/reference/syntax/create_table/statistics.md) clause when creating or altering the table.

## Configuration parameters

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `enable_background_column_stats_collection` | bool | `false` | Collect column statistics in the background |
| `background_analyze_change_ratio_threshold_percent` | uint32 | `20` | Change threshold for early collection, as a percentage |
| `analyze_collect_primary_key_histogram` | bool | `false` | Build a primary-key histogram |
| `analyze_column_table_whole_table_scan_max_bytes` | uint64 | `10737418240` (10 GiB) | Maximum size for scanning a column table as a whole |
| `analyze_row_table_whole_table_scan_max_bytes` | uint64 | `10737418240` (10 GiB) | Maximum size for scanning a row table as a whole |

## Background collection {#enable-background-column-stats-collection}

`enable_background_column_stats_collection: true` enables automatic `ANALYZE` runs for user tables. Background collection is disabled by default.

The next collection is scheduled 24 hours after the start of the previous full `ANALYZE`. This interval is fixed and cannot be configured. Collection may start earlier if the [change threshold](#background-analyze-change-ratio-threshold-percent) is reached. If another table is being processed, collection waits for it to finish.

Sampling (`ANALYZE SAMPLE` with a rate below `1`) does not change the schedule.

### Change threshold {#background-analyze-change-ratio-threshold-percent}

`background_analyze_change_ratio_threshold_percent` sets the threshold for early collection. Row updates and deletes since the last full `ANALYZE` are compared with the current row count.

For example, if the table currently has 1,000 rows, a 20% threshold corresponds to 200 updates or deletes. Repeated changes to the same row are counted separately.

## Primary-key histogram {#analyze-collect-primary-key-histogram}

`analyze_collect_primary_key_histogram: true` enables collection of a primary-key equi-height histogram during `ANALYZE`.

`ANALYZE` must include every primary-key column. No histogram is built if the key is empty or contains a column of an unsupported type.

This setting does not affect histograms defined with [STATISTICS](../../yql/reference/syntax/create_table/statistics.md): `ANALYZE` collects them regardless of its value.

## Reading tables

Size thresholds are specified in bytes and apply only to a full `ANALYZE`. For sampling, see [{#T}](../../yql/reference/syntax/analyze.md#sampling).

### Column tables {#analyze-column-table-whole-table-scan-max-bytes}

If a column table's size is known and does not exceed the threshold, `ANALYZE` scans the whole table without splitting by shard. Larger tables and tables of unknown size are scanned shard by shard.

A value of `0` always uses shard-by-shard scanning.

### Row tables {#analyze-row-table-whole-table-scan-max-bytes}

If a row table's size is known and does not exceed the threshold, `ANALYZE` scans the whole table without splitting into primary-key ranges. Larger tables and tables of unknown size are scanned in primary-key ranges.

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
- [{#T}](../../yql/reference/syntax/create_table/statistics.md)
