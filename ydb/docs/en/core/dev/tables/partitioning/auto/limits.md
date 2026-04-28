# Automatic sharding limits {#auto-sharding-limits}

“Automatic sharding” here means automatic **split/merge** of row-table partitions. Practical constraints include:

1. **Table settings** [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) and [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count): while the partition count stays inside that corridor, {{ ydb-short-name }} can split and merge; when the **maximum** is reached, splitting stops and growth continues via partition size and per-shard load.

2. **Recommended spread between min and max:** if the gap between minimum and maximum is **much larger than ~20%**, [Hive](../../../../concepts/glossary.md#hive) may oscillate between splits and merges under time-varying load. Details and remediation: [{#T}](../../../../troubleshooting/performance/schemas/splits-merges.md).

3. **Decision latency:** the scheme shard may take on the order of **15 seconds** to decide on splitting a tablet — see the troubleshooting article above.

4. **Column-oriented tables:** automatic repartitioning like row tables **is not supported** — partition count must be planned up front.

5. **Secondary indexes:** partitioning settings apply to the **base table**; each index has its own partitions and defaults — see [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table).
