# Excessive tablet splits and merges

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_not_allow_note](../../../_includes/not_allow_for_olap_note.md) %}

{% endif %}

Each [row-oriented table](../../../concepts/datamodel/table.md#row-oriented-tables) partition in {{ ydb-short-name }} is processed by a [data shard](../../../concepts/glossary.md#data-shard) tablet. {{ ydb-short-name }} supports automatic [splitting and merging](../../../concepts/datamodel/table.md#partitioning) of data shards which allows it to seamlessly adapt to changes in workloads. However, these operations are not free and might have a short-term negative impact on query latencies.

When {{ ydb-short-name }} splits a partition, it replaces the original partition with two new partitions covering the same range of primary keys. Now, two data shards process the range of primary keys that was previously handled by a single data shard, thereby adding more computing resources for the table.

By default, {{ ydb-short-name }} splits a table partition when it reaches 2 GB in size. However, it's recommended to also enable partitioning by load, allowing {{ ydb-short-name }} to split overloaded partitions even if they are smaller than 2 GB.

A [scheme shard](../../../concepts/glossary.md#scheme-shard) takes approximately 15 seconds to assess whether a data shard requires splitting. By default, the CPU usage threshold for splitting a data shard is set at 50%.

When {{ ydb-short-name }} merges adjacent partitions in a row-oriented table, they are replaced with a single partition that covers their range of primary keys. TThe corresponding data shards are also consolidated into a single data shard to manage the new partition.

For merging to occur, data shards must have existed for at least 10 minutes, and their CPU usage over the last hour must not exceed 35%.

When configuring [table partitioning](../../../concepts/datamodel/table.md#partitioning), you can also set limits for the [minimum](../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) and [maximum number of partitions](../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count). If the difference between the minimum and maximum limits exceeds 20% and the table load varies significantly over time, [Hive](../../../concepts/glossary.md#hive) may start splitting overloaded tables and then merging them back during periods of low load.

## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/splits-merges.md) %}

## Recommendations

If the user load on {{ ydb-short-name }} has not changed, consider adjusting the gap between the min and max limits for the number of table partitions to the recommended 20% difference. Use the [`ALTER TABLE table_name SET (key = value)`](../../../yql/reference/syntax/alter_table/set.md) YQL statement to update the [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) and [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count) parameters.

If you want to avoid splitting and merging data shards, you can set the min limit to the max limit value or disable partitioning by load.
