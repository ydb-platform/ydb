# Running forced table compaction


```yql
ALTER TABLE table_name COMPACT [WITH (key = value [, ...])];
```


{% if oss == true and backend_name == "YDB" %}

{% include [OLTP_only_allow_note](../../../../_includes/only_allow_for_oltp_note.md) %}

{% endif %}

{{ ydb-short-name }} automatically performs [compaction](../../../../concepts/glossary.md#compaction) of tables and usually does not require manual intervention. However, using the `ALTER TABLE ... COMPACT` command, you can explicitly start compaction of data in a given table without waiting for automatic compaction. The command is useful, for example, in the following scenarios:

* Bulk data deletion was performed and you want to free up the occupied space faster.
* Compression settings or [column groups](family.md) were changed, and you want to apply them to existing data faster.

The command creates a background operation and synchronously waits for its completion. The wait can be interrupted, and the operation will continue running in the background. Background compaction operations can be managed via the [CLI](../../../../reference/ydb-cli/index.md) and [Embedded UI](../../../../reference/embedded-ui/index.md).

## Parameters {#options}

The following parameters can be specified in the `WITH` block:

* `CASCADE` — whether to compact the table together with its [secondary indexes](indexes.md). Valid values: `TRUE` and `FALSE`. Default value: `FALSE` (only the table itself is compacted).
* `PARALLEL` — the maximum number of partitions that can be compacted simultaneously within this operation. Valid values are positive integers. Default value: 1. The overall limit on concurrently running manual compactions for the entire database is set separately in the {{ ydb-short-name }} cluster configuration.

## Behavior {#behavior}

* You can monitor the progress of running compaction operations using the [CLI command](../../../../reference/ydb-cli/operation-list.md) `ydb operation list compaction`. Using the corresponding commands, you can [get the status of a specific operation](../../../../reference/ydb-cli/operation-get.md), [cancel an operation](../../../../reference/ydb-cli/operation-cancel.md), or [delete the record of a completed operation](../../../../reference/ydb-cli/operation-forget.md).
* You can also view and manage operations using the [Embedded UI](../../../../reference/embedded-ui/index.md). To do this, go to the database page, open the `Operations` tab, and select the operation type `Compaction`.
* If a compaction operation is already running for the same table (or for one of its secondary indexes when `CASCADE = TRUE`), the launch will fail with an error. You need to either wait for the previous operation to complete or cancel it.
* Running the command requires the same permissions as other `ALTER TABLE` actions. Concurrent table schema changes are not blocked.
* Forced compaction also materializes borrowed data. As a result, the total data volume in the database may increase. The borrowing mechanism is used, for example, to implement [CoW](https://en.wikipedia.org/wiki/Copy-on-write) [table copying](../../../../reference/ydb-cli/tools-copy.md), so when compacting copied tables, their volume may eventually increase by the size of the original table.

## Examples {#examples}

Run compaction of only the main table:


```yql
ALTER TABLE series COMPACT;
```


Run compaction of the table together with all its secondary indexes:


```yql
ALTER TABLE series COMPACT WITH (CASCADE = TRUE);
```


Run compaction of only the impl table of a specific secondary index:


```yql
ALTER TABLE `series/idx_release/indexImplTable` COMPACT;
```


Run compaction of the table, setting the number of simultaneously processed partitions:


```yql
ALTER TABLE series COMPACT WITH (PARALLEL = 5);
```
