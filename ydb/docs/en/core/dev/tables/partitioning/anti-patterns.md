# Partitioning anti-patterns {#anti-patterns}

Common mistakes in key design and partitioning configuration:

Primary key design anti-patterns (monotonic keys, hot spots) are covered in [{#T}](../../primary-key/row-oriented.md).

* **Cluster-level hard limit on partition size** — **2 GB** by default. If a partition exceeds this limit, it splits unless [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](auto/limits.md) or the [per-table shard limit in the database](../../../concepts/limits-ydb.md#schema-object) is reached.

* **Ignoring `AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`** — once the cap is reached, the table stops splitting and latency/partition size problems grow until the limit is raised.

## Defaults without explicit settings {#default-no-settings}

If you **do not set** `AUTO_PARTITIONING_*` explicitly when creating a row table, model defaults apply (see [{#T}](../../../concepts/datamodel/table.md#partitioning_row_table)).

By default `AUTO_PARTITIONING_BY_LOAD = DISABLED` and `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1` — see the full default list in [{#T}](../../../concepts/datamodel/table.md#partitioning_row_table).

With **high contention on single keys** (hot keys, low cardinality of the leading key component), performance stays limited regardless of `AUTO_PARTITIONING_*` settings — load-based partitioning helps little here; redesign the key (see [{#T}](../../primary-key/row-oriented.md)).

If load is spread more evenly but defaults are not enough, set explicitly:

* [`AUTO_PARTITIONING_BY_LOAD = ENABLED`](../../../concepts/datamodel/table.md#auto_partitioning_by_load);
* [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) and a coherent [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count): **do not set** `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT` below **80%** of `AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`;
* to reduce load on individual partitions, lower the maximum partition size — [`AUTO_PARTITIONING_PARTITION_SIZE_MB`](../../../concepts/datamodel/table.md#auto_partitioning_partition_size_mb).

This avoids “defaults only → single partition → shard overload”.
