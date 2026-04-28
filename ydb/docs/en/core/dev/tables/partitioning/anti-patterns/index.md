# Partitioning anti-patterns {#anti-patterns}

Common mistakes in key design and partitioning configuration:

Primary key design anti-patterns (monotonic keys, hot spots) are covered in [{#T}](../../../primary-key/row-oriented.md).

* **Ignoring `AUTO_PARTITIONING_MAX_PARTITIONS_COUNT`** — once the cap is reached, the table stops splitting and latency/partition size problems grow until the limit is raised.

See [{#T}](default-no-settings.md) for the special case of leaving default partitioning settings unchanged.
