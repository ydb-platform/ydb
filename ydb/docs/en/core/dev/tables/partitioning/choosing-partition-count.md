# Choosing the number of partitions {#choosing-partition-count}

Partition count determines how many [Data shard](../../../concepts/glossary.md#data-shard) tablets process the table in parallel and how data spreads across nodes.

Recommendations from the {{ ydb-short-name }} data model:

* A partition lives on one server and uses **at most one CPU core** for mutations. For tables expected to be hot, set [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) to **at least the node count**, and preferably **well above total CPU cores** allocated to the workload: table load is rarely uniform — many partitions stay idle, and per-partition CPU usage is often closer to a normal distribution than to a flat split.

* [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count) caps partition growth; once the cap is reached, splits stop — monitor and raise the limit before you hit it.[*](#cluster-partition-size-limit)

* With load-based autopartitioning, avoid leaving the minimum partition count at **1** if load is bursty: after a lull, merges can collapse the table to one partition and force expensive splits again. See [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count).

To apply the recommendations:

```yql
ALTER TABLE `my_table` SET (
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10,
    AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 50
);
```

Optional initial spreading across keys uses [`UNIFORM_PARTITIONS`](../../../concepts/datamodel/table.md#uniform_partitions) or [`PARTITION_AT_KEYS`](../../../concepts/datamodel/table.md#partition_at_keys) — see [{#T}](../../../concepts/datamodel/table.md#partitioning_row_table).

---

<span id="cluster-partition-size-limit">\*</span> Regardless of `AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`, the cluster enforces a hard partition size limit (**2 GB** by default): if a partition exceeds it, it splits even when the table-level partition count cap is already reached. See [{#T}](auto/limits.md) and [{#T}](../../../concepts/datamodel/table.md#partitioning_row_table).
