# Size-based {#auto-by-size}

**`AUTO_PARTITIONING_BY_SIZE`** splits a partition when its size exceeds **`AUTO_PARTITIONING_PARTITION_SIZE_MB`**, and merges neighboring partitions when their combined size is low enough (exact rules are in [`AUTO_PARTITIONING_BY_SIZE`](../../../../concepts/datamodel/table.md#auto_partitioning_by_size)).

Practical guidelines:

* **`AUTO_PARTITIONING_PARTITION_SIZE_MB`** is documented with a typical useful range from **tens of MB up to 2000 MB**; pick a value based on workload and acceptable split/merge churn.
* A threshold that is **too high** with skewed key access yields heavy partitions and a hotter single data shard; a threshold that is **too low** causes frequent splits and merges.

Regardless of the user-visible threshold, internal logic also uses a **~2000 MB** guideline for some split decisions — see [{#T}](../../../../concepts/datamodel/table.md#partitioning_row_table).

When lowering the threshold on a **large existing table**, avoid a sudden jump (for example from 2000 MB to 100 MB in one step), which can trigger a wave of splits. Prefer **gradual** decreases and monitor stabilization; see also [{#T}](../../../../troubleshooting/performance/schemas/splits-merges.md).

Only a limited number of split operations can run on a table at once — other partitions wait in a queue. See [{#T}](limits.md).
