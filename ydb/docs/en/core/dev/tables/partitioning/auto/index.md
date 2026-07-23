# Automatic partitioning {#automatic-partitioning}

For row tables, {{ ydb-short-name }} can automatically **split** and **merge** partitions to adapt to data volume and query intensity. This is configured with `AUTO_PARTITIONING_*` table settings (see [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table)).

Typical split/merge duration is on the order of **500 ms**; during that interval data for the affected partition may be briefly unavailable for reads and writes (details in [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table)).

Limits of automatic partitioning (database and table caps, split/merge queue) are covered in [{#T}](limits.md).

## Size-based {#auto-by-size}

**`AUTO_PARTITIONING_BY_SIZE`** splits a partition when its size exceeds **`AUTO_PARTITIONING_PARTITION_SIZE_MB`**, and merges neighboring partitions when their combined size is low enough (exact rules are in [`AUTO_PARTITIONING_BY_SIZE`](../../../../concepts/datamodel/table.md#auto_partitioning_by_size)).

Practical guidelines:

* **`AUTO_PARTITIONING_PARTITION_SIZE_MB`** is documented with a typical useful range from **tens of MB up to 2000 MB**; pick a value based on workload and acceptable split/merge churn.
* A threshold that is **too high** with skewed key access yields heavy partitions and a hotter single data shard; a threshold that is **too low** causes frequent splits and merges.

Regardless of the user-visible threshold, internal logic also uses a **~2000 MB** guideline for some split decisions — see [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table).

When lowering the threshold on a **large existing table**, avoid a sudden jump (for example from 2000 MB to 100 MB in one step), which can trigger a wave of splits. Prefer **gradual** decreases and monitor stabilization; see also [{#T}](../../../../troubleshooting/performance/schemas/splits-merges.md).

Only a limited number of split operations can run on a table at once — other partitions wait in a queue. See [{#T}](limits.md).

## Load-based {#auto-by-load}

**`AUTO_PARTITIONING_BY_LOAD`** splits a partition when it is **CPU saturated**, even if the partition is still small. For row tables this mode is **disabled by default** (`DISABLED`); for highly concurrent writes/reads over a narrow key range you usually want to **enable** it together with [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) set from the expected partition count, so merges do not over-shrink the table during load drops.

Authoritative thresholds, key sampling, replica-aware CPU accounting, and merge conditions are documented in [`AUTO_PARTITIONING_BY_LOAD`](../../../../concepts/datamodel/table.md#auto_partitioning_by_load).

### Typical symptoms of an overloaded partition

* High CPU on individual partitions while overall cluster utilization stays moderate.
* Increased latency on “hot” keys.
* Errors such as **`STATUS_OVERLOADED`** on writes to a hot partition.

A Data shard uses **at most one CPU core** for mutation and read work on a partition: adding CPUs on the node does not remove a single-partition bottleneck — you need splits and/or a key design that spreads load across shards more evenly.
