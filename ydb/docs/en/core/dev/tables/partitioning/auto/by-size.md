# Automatic partitioning by size (size-based) {#auto-by-size}

**`AUTO_PARTITIONING_BY_SIZE`** splits a partition when its size exceeds **`AUTO_PARTITIONING_PARTITION_SIZE_MB`**, and merges neighboring partitions when their combined size is low enough (exact rules are in [{#T}](../../../../concepts/datamodel/table.md#auto_partitioning_by_size)).

Practical guidelines:

* **`AUTO_PARTITIONING_PARTITION_SIZE_MB`** is documented with a typical useful range from **tens of MB up to 2000 MB**; pick a value based on workload and acceptable split/merge churn.
* A threshold that is **too large** can create “heavy” partitions under skewed access; a threshold that is **too small** triggers frequent splits and merges.

Regardless of the user-visible threshold, internal logic also uses a **~2000 MB** guideline for some split decisions — see [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table).

When lowering the threshold on a **large existing table**, avoid a sudden jump (for example from 2000 MB to 100 MB in one step), which can trigger a wave of splits. Prefer **gradual** decreases and monitor stabilization; see also [{#T}](../../../../troubleshooting/performance/schemas/splits-merges.md).
