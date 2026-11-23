# ANALYZE

{% if feature_flags.enable_column_statistics == false %}
{% note warning "Experimental feature" %}

This feature is under development and is disabled by default by the [`enable_column_statistics`](../../../reference/configuration/feature_flags.md) flag. Its behavior may change without backward compatibility, and the feature may never become generally available. Enabling the flag may cause errors, cluster instability, or make it impossible to change the YDB version. Do not enable this flag in production clusters.

{% endnote %}
{% endif %}

`ANALYZE` forces the collection of statistics for [{{ ydb-short-name }} cost-based optimizer](../../../concepts/query_execution/optimizer.md).

## Syntax

```yql
ANALYZE <path_to_table> [ (<column_name> [, ...]) ]
```

This command forces the synchronous collection of table statistics and column statistics for the specified columns or for all columns if none are specified. `ANALYZE` returns once all the requested statistics have been collected and are up to date.

* `path_to_table` — the path to the table for which statistics should be collected.
* `column_name` — collect column statistics only for the specified columns of the table.

The current set of statistics is described in [{#T}](../../../concepts/query_execution/optimizer.md#statistics).
