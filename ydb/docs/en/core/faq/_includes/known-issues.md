# Known issues

## Table partitions are not merging when autopartitioning is enabled {#table-partitions-are-not-merging}

For tables created in earlier versions of {{ydb-short-name}}, the autopartitioning mechanism does not merge partitions even when merge conditions based on load or size are met. This can result in an excessive number of partitions in the table.

This issue is specific to tables that meet the following two criteria:

* The table was created on a {{ydb-short-name}} cluster prior to version 22.2.
* The table had no minimum partition count value set, either at creation time or afterwards.

To resolve the issue, explicitly set a minimum partition count value for the table. For example, use the [AUTO_PARTITIONING_MIN_PARTITIONS_COUNT](../../concepts/datamodel/table.md#auto_partitioning_min_partitions_count) parameter:

```yql
ALTER TABLE `my_table` SET (AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1);
```
