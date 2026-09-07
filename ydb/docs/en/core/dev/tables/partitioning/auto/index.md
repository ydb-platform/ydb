# Automatic partitioning {#automatic-partitioning}

For row tables, {{ ydb-short-name }} can automatically **split** and **merge** partitions to adapt to data volume and query intensity. This is configured with `AUTO_PARTITIONING_*` table settings (see [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table)).

Typical split/merge duration is on the order of **500 ms**; during that interval data for the affected partition may be briefly unavailable for reads and writes (details in [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table)).

Limits of automatic partitioning (database and table caps, split/merge queue) are covered below in [Automatic sharding limits](#auto-sharding-limits).

## Size-based {#auto-by-size}

**`AUTO_PARTITIONING_BY_SIZE`** splits a partition when its size exceeds **`AUTO_PARTITIONING_PARTITION_SIZE_MB`**, and merges neighboring partitions when their combined size is low enough (exact rules are in [`AUTO_PARTITIONING_BY_SIZE`](../../../../concepts/datamodel/table.md#auto_partitioning_by_size)).

Practical guidelines:

* **`AUTO_PARTITIONING_PARTITION_SIZE_MB`** is documented with a typical useful range from **tens of MB up to 2000 MB**; pick a value based on workload and acceptable split/merge churn.
* A threshold that is **too high** with skewed key access yields heavy partitions and a hotter single data shard; a threshold that is **too low** causes frequent splits and merges.

Regardless of the user-visible threshold, internal logic also uses a **~2000 MB** guideline for some split decisions — see [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table).

When changing the threshold on a **large existing table**, avoid an abrupt jump (for example, from 2000 MB to 100 MB in one step). Although the number of concurrent split operations is limited, an abrupt change creates a **long queue** of split tasks. Operations from this queue progress with limited concurrency, which can delay table stabilization. Reduce the threshold gradually and monitor the table state; see also [{#T}](../../../../troubleshooting/performance/schemas/splits-merges.md). For limits on the number of concurrent operations, see [Automatic sharding limits](#auto-sharding-limits).

## Load-based {#auto-by-load}

**`AUTO_PARTITIONING_BY_LOAD`** splits a partition when it is **CPU saturated**, even if the partition is still small. For row tables this mode is **disabled by default** (`DISABLED`); for highly concurrent writes/reads over a narrow key range you usually want to **enable** it together with [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) set from the expected partition count, so merges do not over-shrink the table during load drops.

Authoritative thresholds, key sampling, replica-aware CPU accounting, and merge conditions are documented in [`AUTO_PARTITIONING_BY_LOAD`](../../../../concepts/datamodel/table.md#auto_partitioning_by_load).

### Typical symptoms of an overloaded partition

* High CPU on individual partitions while overall cluster utilization stays moderate.
* Increased latency on “hot” keys.
* An increase in internal **`STATUS_OVERLOADED`** responses. Starting with version 26.2, Query Processor automatically retries these requests, so users usually observe increased latency rather than the error itself.

A Data shard uses **at most one CPU core** for mutation and read work on a partition: adding CPUs on the node does not remove a single-partition bottleneck — you need splits and/or a key design that spreads load across shards more evenly.

## Automatic sharding limits {#auto-sharding-limits}

“Automatic sharding” here means automatic **split/merge** of row-table partitions. Practical constraints come from **database-level** and **table-level** limits, plus internal algorithm behavior.

### Database-level limits

* **Maximum tablets in the database** (`MaxShards`) — once reached, automatic partition splitting stops. See [{#T}](../../../../concepts/limits-ydb.md#schema-object).
* **Maximum table shards** (`MaxShardsInPath`) — upper bound on data shard count for a single table path, including indexes.
* **Cluster hard limit on partition size** — **2 GiB** by default (`ForceShardSplitDataSize`). If a partition exceeds this threshold, it splits even when [`AUTO_PARTITIONING_PARTITION_SIZE_MB`](../../../../concepts/datamodel/table.md#auto_partitioning_partition_size_mb) is higher or [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count) is reached. Details: [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table).

### Table settings

[`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) is the lower bound for automatic merges, while [`AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_max_partitions_count) is the upper bound for regular automatic splits. At the maximum, size-based and load-based splitting stops, but exceeding the 2 GiB hard size limit still triggers a split.

**Recommended spread between min and max:** if the gap between minimum and maximum is **much larger than ~20%**, [Hive](../../../../concepts/glossary.md#hive) may oscillate between splits and merges under time-varying load. Details and remediation: [{#T}](../../../../troubleshooting/performance/schemas/splits-merges.md).

### Load-based adaptation speed

With [`AUTO_PARTITIONING_BY_LOAD`](#auto-by-load) enabled:

* the minimum load-sampling period before a split decision is **two minutes**;
* each split produces **at most two** child partitions.

If load grows by a large factor, partitioning may take **minutes to tens of minutes** to catch up. During this period, overloaded partitions can return internal `OVERLOADED` responses. Starting with version 26.2, Query Processor handles them with automatic retries, so users primarily observe increased latency.

Split and merge operations include partition **compaction** — extra disk I/O.

After sampling is complete, processing the tablet split decision in the scheme shard may take about **15 seconds**. See [{#T}](../../../../troubleshooting/performance/schemas/splits-merges.md).

### Split/merge operation queue

Only a limited number of **split** operations can run on a **table** at once — other partitions wait in a queue. The scheme shard also caps the total number of concurrent split/merge operations in the database.

### Secondary indexes

Partitioning settings apply to the **base table**; each index has its own partitions and defaults — see [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table).

{% note warning %}

Column-oriented tables: automatic repartitioning like row tables **is not supported** — partition count must be planned up front. See [{#T}](../column-oriented.md).

{% endnote %}
