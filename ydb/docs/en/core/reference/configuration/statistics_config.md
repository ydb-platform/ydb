# statistics_config

{% if feature_flags.enable_column_statistics == false %}
{% note warning "Experimental feature" %}

This feature is under development and is disabled by default by the [`enable_column_statistics`](feature_flags.md) flag. Its behavior may change without backward compatibility, and the feature may never become generally available. Enabling the flag may cause errors, cluster instability, or make it impossible to change the YDB version. Do not enable this flag in production clusters. [Learn more](feature_flags.md#experimental).

{% endnote %}
{% endif %}

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
