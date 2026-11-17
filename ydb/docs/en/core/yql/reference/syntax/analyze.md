# ANALYZE

{% if feature_flags.enable_column_statistics == false %}
{% note warning "Experimental feature" %}

This feature is under development and is disabled by default by the [`enable_column_statistics`](../../../reference/configuration/feature_flags.md) flag. Its behavior may change without backward compatibility, and the feature may never become generally available. Enabling the flag may cause errors, cluster instability, or make it impossible to change the YDB version. Do not enable this flag in production clusters. [Learn more](../../../reference/configuration/feature_flags.md#experimental).

{% endnote %}
{% endif %}

`ANALYZE` collects statistics for a table and its columns. The [{{ ydb-short-name }} cost-based optimizer](../../../concepts/query_execution/optimizer.md) uses them to estimate how much data a query will process and choose an execution plan.

The statement works with one table and completes after updating the statistics.

## Syntax

```yql
ANALYZE <path_to_table> [ (<column_name> [, ...]) ]
```

Parameters:

* `path_to_table`: path to the table.
* `column_name`: columns to collect statistics for. If the list is omitted, all columns are processed.

## Examples

Collect statistics for the `orders` table:

```yql
-- All columns
ANALYZE orders;

-- Selected columns
ANALYZE orders (customer_id, amount);
```

## Collected statistics

`ANALYZE` collects statistics for selected columns: estimates of the number of distinct values, their frequencies, and their distribution. The statistics collected depend on the column's type and contents.
