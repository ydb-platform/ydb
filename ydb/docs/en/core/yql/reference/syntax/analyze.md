# ANALYZE

<<<<<<< HEAD
`ANALYZE` forces the collection of statistics for [{{ ydb-short-name }} cost-based optimizer](../../../concepts/query_execution/optimizer.md).
=======
{% if feature_flags.enable_column_statistics == false %}
{% note warning "Experimental feature" %}

This feature is under development and is disabled by default by the [`enable_column_statistics`](../../../reference/configuration/feature_flags.md) flag. Its behavior may change without backward compatibility, and the feature may never become generally available. Enabling the flag may cause errors, cluster instability, or make it impossible to change the YDB version. Do not enable this flag in production clusters. [Learn more](../../../reference/configuration/feature_flags.md#experimental).

{% endnote %}
{% endif %}

`ANALYZE` collects statistics for a table and its columns. The [{{ ydb-short-name }} cost-based optimizer](../../../concepts/query_execution/optimizer.md) uses them to estimate how much data a query will process and choose an execution plan.

The statement works with one table and completes after updating the statistics.
>>>>>>> 6f22a4d5a17 (add description about feature flag enable_column_statistics)

## Syntax

```yql
ANALYZE <path_to_table> [ (<column_name> [, ...]) ]
```

This command forces the synchronous collection of table statistics and column statistics for the specified columns or for all columns if none are specified. `ANALYZE` returns once all the requested statistics have been collected and are up to date.

* `path_to_table` — the path to the table for which statistics should be collected.
* `column_name` — collect column statistics only for the specified columns of the table.

The current set of statistics is described in [{#T}](../../../concepts/query_execution/optimizer.md#statistics).

## Background operation

SQL operation `ANALYZE` executes synchronously and waits for completion. In addition a background operation is created for observability. Use the [operation list](../../../reference/ydb-cli/operation-list.md) command to monitor the progress of background `ANALYZE` operations.
