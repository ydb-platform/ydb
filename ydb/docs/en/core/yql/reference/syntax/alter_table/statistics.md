# STATISTICS

{% if feature_flags.enable_column_statistics == false %}
{% note warning "Experimental feature" %}

This feature is under development and is disabled by default by the [`enable_column_statistics`](../../../../reference/configuration/feature_flags.md) flag. Its behavior may change without backward compatibility, and the feature may never become generally available. Enabling the flag may cause errors, cluster instability, or make it impossible to change the YDB version. Do not enable this flag in production clusters. [Learn more](../../../../reference/configuration/feature_flags.md#experimental).

{% endnote %}
{% endif %}

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
