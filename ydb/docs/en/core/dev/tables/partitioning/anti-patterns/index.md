# Partitioning anti-patterns {#anti-patterns}

Common mistakes in key design and partitioning configuration:

* **Low cardinality of the leading primary-key component** — for example `(status, id)` where `status` is almost always the same: most rows land in one range and one partition stays hot; autopartitioning cannot fix skewed access evenly.

* **Hot keys** — most traffic hits a narrow key range. Even with [`AUTO_PARTITIONING_BY_LOAD`](../auto/by-load.md) enabled, redesigning the key and access pattern often helps; see [{#T}](../../../primary-key/row-oriented.md).

* **Ignoring `AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`** — once the cap is reached, the table stops splitting and latency/partition size problems grow until the limit is raised.

See [{#T}](default-no-settings.md) for the special case of leaving default partitioning settings unchanged.
