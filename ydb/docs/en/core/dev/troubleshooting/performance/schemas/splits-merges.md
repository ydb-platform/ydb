# Excessive tablet splits and merges

{% note warning %}

Tablet splitting and merging is supported only for row-oriented tables.

{% endnote %}


Each table partition in {{ ydb-short-name }} is processed by a [data shard](../../../../concepts/glossary.md#data-shard) tablet. When {{ ydb-short-name }} splits a partition in a row-oriented table, two new partitions replace the original partition to cover the range of primary keys. So two data shards are now processing the range of primary keys that were previously processed by only one data shard, adding more computing resources for the table.

By default, {{ ydb-short-name }} splits a table partition when it reaches the size of 2GB. But it's recommended to also enable partitioning by load, so that {{ ydb-short-name }} splits overloaded partitions even if they are smaller than 2 GBs.

It takes a [scheme shard](../../../../concepts/glossary.md#scheme-shard) approximately 15 seconds to determine if a data shard needs splitting. By default, the CPU usage threshold for splitting a data shard is 50%.

When {{ ydb-short-name }} merges adjacent partitions in a row-oriented table, they are replaced with a single partition that covers their range of primary keys. The corresponding data shards are also merged into one data shard to process the new partition.

Data shards must have existed for at least 10 minutes before they can be merged. Besides, the shard's CPU usage over the last hour must not exceed 35%.

When you configure [table partitioning](../../../../concepts/datamodel/table.md#partitioning), you can also set the limits for the [minimum](../../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) and [maximum number of partitions](../../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count). If the difference between the minimum and maximum values exceeds 20% and the load on the table varies greatly over time, [Hive](../../../../concepts/glossary.md#hive) may start splitting overloaded tables and then merging them back under low load.

## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/splits-merges.md) %}

## Recommendations

If the user load on {{ ydb-short-name }} has not changed, consider adjusting the gap between the min and max limits for the number of table partitions to the recommended 20% difference.

If you want to avoid splitting and merging data shards, you can set the min limit to the max limit value or disable partitioning by load.
