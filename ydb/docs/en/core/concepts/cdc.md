# Change Data Capture (CDC)

{% include [olap_not_allow](../_includes/not_allow_for_olap_note.md) %}

Change Data Capture (CDC) captures changes to {{ ydb-short-name }} table rows, uses these changes to generate a _changefeed_, writes them to distributed storage, and provides access to these records for further processing. It uses a [topic](topic.md) as distributed storage to efficiently store the table change log.

When adding, updating, or deleting a table row, CDC generates a change record by specifying the [primary key](datamodel/table.md) of the row and writes it to the topic partition corresponding to this key.

## Guarantees {#guarantees}

* Change records are sharded across topic partitions by primary key.
* Each change is only delivered once (exactly-once delivery).
* Changes by the same primary key are delivered to the same topic partition in the order they took place in the table.
* Change record is delivered to the topic partition only after the corresponding transaction in the table has been committed.

## Limitations {#restrictions}

* The number of topic partitions is fixed as of changefeed creation and remains unchanged (unlike tables, topics are not elastic).
* Changefeeds support records of the following types of operations:
   * Updates
   * Erases

   Adding rows is a special update case, and a record of adding a row in a changefeed will look similar to an update record.

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

### JSON format {#json-record-structure}

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
* `erase`: Erase flag. Present if a record matches the erase operation.
* `newImage`: Row snapshot that results from its being changed. Present in `NEW_IMAGE` and `NEW_AND_OLD_IMAGES` modes. Contains column names and values.
* `oldImage`: Row snapshot before the change. Present in `OLD_IMAGE` and `NEW_AND_OLD_IMAGES` modes. Contains column names and values.
* `ts`: [Virtual timestamp](#virtual-timestamps). Present if the `VIRTUAL_TIMESTAMPS` setting is enabled. Contains the value of the global coordinator time (`step`) and the unique transaction ID (`txId`).

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
>    "ts": [1670792400890, 562949953607163]
> }
> ```

{% note info %}

* The same record may not contain the `update` and `erase` fields simultaneously, since these fields are operation flags (you can't update and erase a table row at the same time). However, each record contains one of these fields (any operation is either an update or an erase).
* In `UPDATES` mode, the `update` field for update operations is an operation flag (update) and contains the names and values of updated columns.
* JSON object fields containing column names and values (`newImage`, `oldImage`, and `update` in `UPDATES` mode), *do not include* the columns that are primary key components.
* If a record contains the `erase` field (indicating that the record matches the erase operation), this is always an empty JSON object (`{}`).

{% endnote %}

{% if audience == "tech" %}

### Amazon DynamoDB-compatible JSON format {#dynamodb-streams-json-record-structure}

For [Amazon DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html)-compatible document tables, {{ ydb-short-name }} can generate change records in the [Amazon DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html)-compatible format.

The record structure is the same as for [Amazon DynamoDB Streams](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_Record.html) records:
* `awsRegion`: Includes the string delivered in the `AWS_REGION` option when creating a changefeed.
* `dynamodb`: [StreamRecord](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_StreamRecord.html).
* `eventID`: Unique record ID.
* `eventName`: `INSERT`, `MODIFY`, or `REMOVE`. You can only use `INSERT` in the `NEW_AND_OLD_IMAGES` mode.
* `eventSource`: Includes the `ydb:document-table` string.
* `eventVersion`: Includes the `1.0` string.

{% endif %}

### Debezium-compatible JSON format {#debezium-json-record-structure}

A [Debezium](https://debezium.io)-compatible JSON record structure has the following format:

```json
{
    "payload": {
        "op": <op>,
        "before": {<columns>},
        "after": {<columns>},
        "source": {
            "connector": <connector>,
            "version": <version>,
            "ts_ms": <ts_ms>,
            "step": <step>,
            "txId": <txId>,
            "snapshot": <bool>
        }
    }
}
```

* `op`: Operation that was performed on a row:
  * `c` — create. Applicable only in `NEW_AND_OLD_IMAGES` mode.
  * `u` — update.
  * `d` — delete.
  * `r` — read from [snapshot](#initial-scan).
* `before`: Row snapshot before the change. Present in `OLD_IMAGE` and `NEW_AND_OLD_IMAGES` modes. Contains column names and values.
* `after`: Row snapshot after the change. Present in `NEW_IMAGE` and `NEW_AND_OLD_IMAGES` modes. Contains column names and values.
* `source`: Source metadata for the event.
  * `connector`: Connector name. Current name is `ydb`.
  * `version`: Connector version that was used to generate the record. Current version is `1.0.0`.
  * `ts_ms`: Approximate time when the change was applied, in milliseconds.
  * `step`: Global coordinator time. Part of the [virtual timestamp](#virtual-timestamps).
  * `txId`: Unique transaction ID. Part of the [virtual timestamp](#virtual-timestamps).
  * `snapshot`: Whether the event is part of a snapshot.

When reading using Kafka API, the Debezium-compatible primary key of the modified row is specified as the message key:

```json
{
    "payload": {<columns>}
}
```

* `payload`: Key of a row that was changed. Contains names and values of the columns that are components of the primary key.

## Record retention period {#retention-period}

By default, records are stored in the changefeed for 24 hours from the time they are sent. Depending on usage scenarios, the retention period can be reduced or increased up to 30 days.

{% note warning %}

Records whose retention time has expired are deleted, regardless of whether they were processed (read) or not.

{% endnote %}

Deleting records before they are processed by the client will cause [offset](topic.md#offset) skips, which means that the offsets of the last record read from the partition and the earliest available record will differ by more than one.

To set up the record retention period, specify the [RETENTION_PERIOD](../yql/reference/syntax/alter_table.md#changefeed-options) parameter when creating a changefeed.

## Topic partitions {#topic-partitions}

By default, the number of [topic partitions](topic.md#partitioning) is equal to the number of table partitions. The number of topic partitions can be redefined by specifying [TOPIC_MIN_ACTIVE_PARTITIONS](../yql/reference/syntax/alter_table.md#changefeed-options) parameter when creating a changefeed.

{% note info %}

Currently, the ability to explicitly specify the number of topic partitions is available only for tables whose first primary key component is of type `Uint64` or `Uint32`.

{% endnote %}

## Creating and deleting a changefeed {#ddl}

You can add a changefeed to an existing table or erase it using the [ADD CHANGEFEED and DROP CHANGEFEED](../yql/reference/syntax/alter_table.md#changefeed) directives of the YQL `ALTER TABLE` statement. When erasing a table, the changefeed added to it is also deleted.

## CDC purpose and use {#best_practices}

For information about using CDC when developing apps, see [best practices](../dev/cdc.md).
