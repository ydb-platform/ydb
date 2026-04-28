# Defaults without explicit settings {#default-no-settings}

If you **do not set** `AUTO_PARTITIONING_*` explicitly when creating a row table, model defaults apply (see [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table)):

* **`AUTO_PARTITIONING_BY_SIZE`** is typically **enabled** — splits by partition size using the default **`AUTO_PARTITIONING_PARTITION_SIZE_MB`** (on the order of 2 GB in documentation).

* **`AUTO_PARTITIONING_BY_LOAD`** defaults to **disabled** — CPU overload **will not** trigger automatic splits until you enable the mode.

* **`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`** defaults to **1** — after load drops, merges can leave a single partition; the next spike may require splits again.

For tables with **predictably high** contention on a narrow key or heavy write rates, set explicitly:

* [`AUTO_PARTITIONING_BY_LOAD = ENABLED`](../../../../concepts/datamodel/table.md#auto_partitioning_by_load);
* [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) **> 1** with a coherent [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count);
* if needed, a more appropriate [`AUTO_PARTITIONING_PARTITION_SIZE_MB`](../../../../concepts/datamodel/table.md#auto_partitioning_partition_size_mb).

This avoids “defaults only → single partition → shard overload”.
