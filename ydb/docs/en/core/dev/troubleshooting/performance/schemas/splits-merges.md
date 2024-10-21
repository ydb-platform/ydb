# Excessive tablet splits and merges

Each table partition in {{ ydb-short-name }} is processed by a [data shard](../../../../concepts/glossary.md#data-shard) tablet. When {{ ydb-short-name }} splits a partition, a new data shard is added to process the new partition, thus adding more computing resources for the table.

By default, {{ ydb-short-name }} splits a table partition when it reaches the size of 2GB. But it's recommended to also enable partitioning by load, so that {{ ydb-short-name }} splits overloaded partitions even if they are smaller than 2 GBs.

When you configure [table partitioning](../../../../concepts/datamodel/table.md#partitioning), you can also set the limits for the [minimum](../../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) and [maximum number of partitions](../../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count). If the difference between the minimum and maximum values exceeds 20%, Hive may start splitting tables under minimal load and then merging them back when the load is reduced.

<!-- TODO: Add decision time to merge -->

## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/splits-merges.md) %}

## Recommendations

If the user load on {{ ydb-short-name }} has not changed, consider adjusting the gap between the min and max limits for the number of table partitions.
