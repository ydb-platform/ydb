# Automatic sharding limits {#auto-sharding-limits}

“Automatic sharding” here means automatic **split/merge** of row-table partitions. Practical constraints include:

1. **Table settings** [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) and [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count): while the partition count stays inside that corridor, {{ ydb-short-name }} can split and merge; when the **maximum** is reached, splitting stops and growth continues via partition size and per-shard load.

2. **Split/merge cycles:** with an incorrectly configured min–max corridor, spurious split and merge cycles are possible. For causes, symptoms, and remediation: [{#T}](../../../../troubleshooting/performance/schemas/splits-merges.md).

3. **Column-oriented tables:** automatic repartitioning like row tables **is not supported** — partition count must be planned up front.

4. **Secondary indexes:** partitioning settings apply to the **base table**; each index has its own partitions and defaults — see [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table).
