# Choosing the number of partitions {#choosing-partition-count}

Partition count determines how many [Data shard](../../../concepts/glossary.md#data-shard) tablets process the table in parallel and how data spreads across nodes.

Recommendations from the {{ ydb-short-name }} data model:

* A partition lives on one server and uses **at most one CPU core** for mutations. For tables expected to be hot, set [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) to **at least the node count**, and preferably **on the order of total CPU cores** allocated to the workload.

* [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count) caps partition growth; once the cap is reached, splits stop — monitor and raise the limit before you hit it.

* With load-based autopartitioning, avoid leaving the minimum partition count at **1** if load is bursty: after a lull, merges can collapse the table to one partition and force expensive splits again. See [{#T}](../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count).

Optional initial spreading across keys uses [`UNIFORM_PARTITIONS`](../../../concepts/datamodel/table.md#uniform_partitions) or [`PARTITION_AT_KEYS`](../../../concepts/datamodel/table.md#partition_at_keys) — see [{#T}](../../../concepts/datamodel/table.md#partitioning_row_table).
