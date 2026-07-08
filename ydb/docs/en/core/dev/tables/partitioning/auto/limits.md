# Automatic sharding limits {#auto-sharding-limits}

“Automatic sharding” here means automatic **split/merge** of row-table partitions. Practical constraints come from **database-level** and **table-level** limits, plus internal algorithm behavior.

## Database-level limits

* **Maximum tablets in the database** (`MaxShards`) — once reached, automatic partition splitting stops. See [{#T}](../../../../concepts/limits-ydb.md#schema-object).
* **Maximum table shards** (`MaxShardsInPath`) — upper bound on data shard count for a single table path, including indexes.
* **Cluster hard limit on partition size** — **2 GB** by default (`ForceShardSplitDataSize`). If a partition exceeds this threshold, it splits even when [`AUTO_PARTITIONING_PARTITION_SIZE_MB`](../../../../concepts/datamodel/table.md#auto_partitioning_partition_size_mb) is higher or [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count) is reached. Details: [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table).

## Table settings

While the partition count stays inside [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) — [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count), {{ ydb-short-name }} can **split** and **merge** partitions; when the **maximum** is reached, splitting stops and growth continues via partition size and per-shard load.

**Recommended spread between min and max:** if the gap between minimum and maximum is **much larger than ~20%**, [Hive](../../../../concepts/glossary.md#hive) may oscillate between splits and merges under time-varying load. Details and remediation: [{#T}](../../../../troubleshooting/performance/schemas/splits-merges.md).

## Load-based adaptation speed

With [`AUTO_PARTITIONING_BY_LOAD`](by-load.md) enabled:

* the decision to split a partition by load takes **at least about two minutes**;
* each split produces **at most two** child partitions.

If load grows by a large factor, partitioning may take **minutes to tens of minutes** to catch up; overloaded partitions may return `OVERLOADED` errors — both from saturation and from the split process itself.

Split and merge operations include partition **compaction** — extra disk I/O.

The scheme shard may take on the order of **15 seconds** to decide on splitting a tablet — see the [{#T}](../../../../troubleshooting/performance/schemas/splits-merges.md) article.

## Split/merge operation queue

Only a limited number of **split** operations can run on a **table** at once — other partitions wait in a queue. The scheme shard also caps the total number of concurrent split/merge operations in the database.

## Secondary indexes

Partitioning settings apply to the **base table**; each index has its own partitions and defaults — see [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table).

{% note warning %}

Column-oriented tables: automatic repartitioning like row tables **is not supported** — partition count must be planned up front. See [{#T}](../../../primary-key/column-oriented.md).

{% endnote %}
