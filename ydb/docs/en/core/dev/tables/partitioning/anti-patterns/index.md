# Partitioning anti-patterns {#anti-patterns}

Common mistakes in key design and partitioning configuration:

Primary key design anti-patterns (monotonic keys, hot spots) are covered in [{#T}](../../../primary-key/row-oriented.md).

* **Cluster-level hard limit on partition size** — **2 GB** by default. If a partition exceeds this limit, it splits unless [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../auto/limits.md) or the [per-table shard limit in the database](../../../../concepts/limits-ydb.md#schema-object) is reached.

* **Ignoring `AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`** — once the cap is reached, the table stops splitting and latency/partition size problems grow until the limit is raised.

For default partitioning settings without explicit configuration, see [{#T}](default-no-settings.md).
