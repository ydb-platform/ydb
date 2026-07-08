# Load-based {#auto-by-load}

**`AUTO_PARTITIONING_BY_LOAD`** splits a partition when it is **CPU saturated**, even if the partition is still small. For row tables this mode is **disabled by default** (`DISABLED`); for highly concurrent writes/reads over a narrow key range you usually want to **enable** it together with [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](../../../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) set from the expected partition count, so merges do not over-shrink the table during load drops.

Authoritative thresholds, key sampling, replica-aware CPU accounting, and merge conditions are documented in [`AUTO_PARTITIONING_BY_LOAD`](../../../../concepts/datamodel/table.md#auto_partitioning_by_load).

## Typical symptoms of an overloaded partition

* High CPU on individual partitions while overall cluster utilization stays moderate.
* Increased latency on “hot” keys.
* Errors such as **`STATUS_OVERLOADED`** on writes to a hot partition.

A Data shard uses **at most one CPU core** for mutation and read work on a partition: adding CPUs on the node does not remove a single-partition bottleneck — you need splits and/or a key design that spreads load across shards more evenly.
