# Change Data Capture (CDC)

Change Data Capture (CDC) captures changes to {{ ydb-short-name }} table rows, uses these changes to generate a _changefeed_, writes them to distributed storage, and provides access to these records for further processing. It uses a [topic](topic.md) as distributed storage to efficiently store the table change log.

When adding, updating, or deleting a table row, CDC generates a change record by specifying the [primary key](datamodel/table.md) of the row and writes it to the topic partition corresponding to this key.

## Guarantees {#guarantees}

* Change records are sharded across topic partitions by primary key.
* Each change is only delivered once (exactly-once delivery).
* Changes by the same primary key are delivered to the same topic partition in the order they took place in the table.

## Limitations {#restrictions}

* The number of topic partitions is fixed as of changefeed creation and remains unchanged (unlike tables, topics are not elastic).
* Changefeeds support records of the following types of operations:
   * Updates
   * Deletes

   Adding rows is a special case of updates, and a record of adding a row in a changefeed will look similar to an update record.

## Virtual timestamps {#virtual-timestamps}

All changes in {{ ydb-short-name }} tables are arranged according to the order in which transactions are performed. Each change is marked with a virtual timestamp which consists of two elements:

1. Global coordinator time.
1. Unique transaction ID.

Using these stamps, you can arrange records from different partitions of the topic relative to each other or use them for filtering (for example, to exclude old change records).

{% note info %}

By default, virtual timestamps are not uploaded to the changefeed. To enable them, use the [appropriate parameter](../yql/reference/syntax/alter_table.md#changefeed-options) when creating a changefeed.

{% endnote %}

## Initial table scan {#initial-scan}

By default, a changefeed only includes records about those table rows that changed after the changefeed was created. Initial table scan enables you to export, to the changefeed, the values of all the rows that existed at the time of changefeed creation.

The scan runs in the background mode on top of the table snapshot. The following situations are possible:
* A non-scanned row changes in the table. The changefeed will receive, one after another: a record with the source value and a record about the update.  When the same record is changed again, only the update record is exported.
* A changed row is found during scanning. Nothing is exported to the changefeed because the source value has already been exported at the time of change (see the previous paragraph).
* A scanned row changes in the table. Only an update record exports to the changefeed.

This ensures that, for the same row (the same primary key), the source value is exported first, and then the updated value is exported.

{% note info %}

The record with the source row value is labeled as an [update](#restrictions) record. When using [virtual timestamps](#virtual-timestamps), records are marked by the snapshot's timestamp.

{% endnote %}

During the scanning process, depending on the table update frequency, you might see too many `OVERLOADED` errors. This is because, besides the update records, you also need to deliver records with the source row values. When the scan is complete, the changefeed switches to normal operation.

## Record structure {#record-structure}

Depending on the [changefeed parameters](../yql/reference/syntax/alter_table.md#changefeed-options), the structure of a record may differ.

A [JSON](https://en.wikipedia.org/wiki/JSON) record has the following structure:

```json
{
    "key": [<key components>],
    "update": {<columns>},
    "erase": {},
    "newImage": {<columns>},
    "oldImage": {<columns>},
    "ts": [<step>, <txId>]
}
```

* `key`: An array of primary key component values. Always present.
* `update`: Update flag. Present if a record matches the update operation. In `UPDATES` mode, it also contains the names and values of updated columns.
* `erase`: Erase flag. Present if a record matches the delete operation.
* `newImage`: Row snapshot that results from its change. Present in `NEW_IMAGE` and `NEW_AND_OLD_IMAGES` modes. Contains column names and values.
* `oldImage`: Row snapshot before its change. Present in `OLD_IMAGE` and `NEW_AND_OLD_IMAGES` modes. Contains column names and values.
* `ts`: Virtual timestamp. Present if the `VIRTUAL_TIMESTAMPS` setting is enabled. Contains the value of the global coordinator time (`step`) and the unique transaction ID (`txId`).

> Sample record of an update in `UPDATES` mode:
>
> ```json
> {
>    "key": [1, "one"],
>    "update": {
>        "payload": "lorem ipsum",
>        "date": "2022-02-22"
>    }
> }
> ```
>
> Record of an erase:
> ```json
> {
>    "key": [2, "two"],
>    "erase": {}
> }
> ```
>
> Record with row snapshots:
> ```json
> {
>    "key": [1, 2, 3],
>    "update": {},
>    "newImage": {
>        "textColumn": "value1",
>        "intColumn": 101,
>        "boolColumn": true
>    },
>    "oldImage": {
>        "textColumn": null,
>        "intColumn": 100,
>        "boolColumn": false
>    }
> }
> ```
>
> Record with virtual timestamps:
> ```json
> {
>    "key": [1],
>    "update": {
>        "created": "2022-12-12T00:00:00.000000Z",
>        "customer": "Name123"
>    },
>    "ts": [1670792400, 562949953607163]
> }
> ```

{% note info %}

* The same record may not contain the `update` and `erase` fields simultaneously, since these fields are operation flags (you can't update and erase a table row at the same time). However, each record contains one of these fields (any operation is either an update or erase).
* In `UPDATES` mode, the `update` field for update operations is an operation flag (update) and contains the names and values of updated columns.
* JSON object fields containing column names and values (`newImage`, `oldImage`, and `update` in `UPDATES` mode), *do not include* the columns that are primary key components.
* If a record contains the `erase` field (indicating that the record matches the erase operation), this is always an empty JSON object (`{}`).

{% endnote %}

## Record retention period {#retention-period}

By default, records are stored in the changefeed for 24 hours from the time they are sent. Depending on usage scenarios, the retention period can be reduced or increased up to 30 days.

{% note warning %}

Records whose retention time has expired are deleted, regardless of whether they were processed (read) or not.

{% endnote %}

Deleting records before they are processed by the client will cause [offset](topic.md#offset) skips, which means that the offsets of the last record read from the partition and the earliest available record will differ by more than one.

To set up the record retention period, specify the [RETENTION_PERIOD](../yql/reference/syntax/alter_table.md#changefeed-options) parameter when creating a changefeed.

## Creating and deleting a changefeed {#ddl}

You can add a changefeed to an existing table or delete it using the [ADD CHANGEFEED and DROP CHANGEFEED](../yql/reference/syntax/alter_table.md#changefeed) directives of the YQL `ALTER TABLE` statement. When deleting a table, the changefeed added to it is also deleted.

## CDC purpose and use {#best_practices}

For information about using CDC when developing apps, see [best practices](../best_practices/cdc.md).
