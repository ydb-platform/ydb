# statistics_config

The `statistics_config` section controls how column statistics are collected for the [cost-based optimizer](../../concepts/query_execution/optimizer.md#statistics). Row counts and table sizes come from scheme statistics and are not configured here.

Run [ANALYZE](../../yql/reference/syntax/analyze.md) to collect statistics on demand. Declare multi-column statistics with [STATISTICS](../../yql/reference/syntax/create_table/statistics.md).

## Configuration parameters

| Parameter | Type | Default | Description |
|:----------|:-----|:--------|:------------|
| `enable_background_column_stats_collection` | bool | `false` | Collect column statistics in the background |
| `background_analyze_change_ratio_threshold_percent` | uint32 | `20` | Percentage of rows updated or deleted since the last full `ANALYZE` at which column statistics are considered stale |
| `analyze_collect_primary_key_histogram` | bool | `false` | Build an equi-height histogram of the primary key on a full `ANALYZE` |
| `analyze_column_table_whole_table_scan_max_bytes` | uint64 | `10737418240` (10 GiB) | Maximum size of a column table that `ANALYZE` reads in a single query |
| `analyze_row_table_whole_table_scan_max_bytes` | uint64 | `10737418240` (10 GiB) | Maximum size of a row table that `ANALYZE` reads in a single query |

### enable_background_column_stats_collection {#enable-background-column-stats-collection}

When this is `true`, {{ ydb-short-name }} runs `ANALYZE` automatically for user tables. Each table is usually scanned about once a day; the threshold configured by [`background_analyze_change_ratio_threshold_percent`](#background-analyze-change-ratio-threshold-percent) can trigger a scan sooner. `ANALYZE SAMPLE` does not reset the background collection schedule. The internal table `.metadata/statistics_v2` is excluded from the schedule.

### background_analyze_change_ratio_threshold_percent {#background-analyze-change-ratio-threshold-percent}

The threshold is a percentage of the current row count. Column statistics are considered stale when the percentage of rows updated or deleted since the last full `ANALYZE` reaches it:

`(row updates + row deletes since the last full ANALYZE) / row count × 100%`

The default is 20%.

### analyze_collect_primary_key_histogram {#analyze-collect-primary-key-histogram}

When this is `true`, a full `ANALYZE` builds an equi-height histogram of the primary key. It is skipped if the column list omits a key column, or if the key is empty or unsupported. This setting is `false` by default. A histogram declared as `STATISTICS ... WITH (EQ_HEIGHT_HISTOGRAM)` is collected regardless of this setting.

### analyze_column_table_whole_table_scan_max_bytes {#analyze-column-table-whole-table-scan-max-bytes}

If a column table's size is known and within this limit, `ANALYZE` reads the whole table in one query. Larger tables and tables of unknown size are scanned shard by shard. A value of `0` always uses shard-by-shard scanning. The default is 10 GiB (`10737418240`).

`ANALYZE SAMPLE` with a rate below `1` always reads only a subset of shards. The size limit applies to a full `ANALYZE`.

### analyze_row_table_whole_table_scan_max_bytes {#analyze-row-table-whole-table-scan-max-bytes}

A row table whose size is known and within this limit is read in one query. Larger tables and tables of unknown size are scanned in primary-key ranges. A value of `0` scans by primary-key range when the key allows it. Otherwise the whole table is read. The default is 10 GiB (`10737418240`).

`ANALYZE SAMPLE` with a rate below `1` always uses a single scan. The size limit applies to a full `ANALYZE`.

## Configuration example

```yaml
statistics_config:
  enable_background_column_stats_collection: true
  background_analyze_change_ratio_threshold_percent: 20
  analyze_collect_primary_key_histogram: false
```

## See also

- [{#T}](../../concepts/query_execution/optimizer.md#statistics)
- [{#T}](../../yql/reference/syntax/analyze.md)
- [{#T}](../../yql/reference/syntax/create_table/statistics.md)
