# Defaults without explicit settings {#default-no-settings}

If you **do not set** `AUTO_PARTITIONING_*` explicitly when creating a row table, model defaults apply (see [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table)):

By default `AUTO_PARTITIONING_BY_LOAD = DISABLED` and `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1` — see the full default list in [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table).

For tables with **predictably high** contention on a narrow key or heavy write rates, set explicitly:

* [`AUTO_PARTITIONING_BY_LOAD = ENABLED`](../../../../concepts/datamodel/table.md#auto_partitioning_by_load);
* [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) **> 1** with a coherent [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count);
* if needed, a more appropriate [`AUTO_PARTITIONING_PARTITION_SIZE_MB`](../../../../concepts/datamodel/table.md#auto_partitioning_partition_size_mb).

This avoids “defaults only → single partition → shard overload”.
