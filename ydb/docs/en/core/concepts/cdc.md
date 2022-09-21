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

## Record structure {#record-structure}

Depending on the [changefeed parameters](../yql/reference/syntax/alter_table.md#changefeed-options), the structure of a record may differ.

A [JSON](https://en.wikipedia.org/wiki/JSON) record has the following structure:

```json
{
    "key": [<key components>],
    "update": {<columns>},
    "erase": {},
    "newImage": {<columns>},
    "oldImage": {<columns>}
}
```

* `key`: An array of primary key component values. Always present.
* `update`: Update flag. Present if a record matches the update operation. In `UPDATES` mode, it also contains the names and values of updated columns.
* `erase`: Erase flag. Present if a record matches the delete operation.
* `newImage`: Row snapshot that results from its change. Present in `NEW_IMAGE` and `NEW_AND_OLD_IMAGES` modes. Contains column names and values.
* `oldImage`: Row snapshot before its change. Present in `OLD_IMAGE` and `NEW_AND_OLD_IMAGES` modes. Contains column names and values.

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

{% note info %}

* The same record may not contain the `update` and `erase` fields simultaneously, since these fields are operation flags (you can't update and erase a table row at the same time). However, each record contains one of these fields (any operation is either an update or erase).
* In `UPDATES` mode, the `update` field for update operations is an operation flag (update) and contains the names and values of updated columns.
* JSON object fields containing column names and values (`newImage`, `oldImage`, and `update` in `UPDATES` mode), *do not include* the columns that are primary key components.
* If a record contains the `erase` field (indicating that the record matches the erase operation), this is always an empty JSON object (`{}`).

{% endnote %}

## Creating and deleting a changefeed {#ddl}

You can add a changefeed to an existing table or delete it using the [ADD CHANGEFEED and DROP CHANGEFEED](../yql/reference/syntax/alter_table.md#changefeed) directives of the YQL `ALTER TABLE` statement. When deleting a table, the changefeed added to it is also deleted.

## CDC purpose and use {#best_practices}

For information about using CDC when developing apps, see [best practices](../best_practices/cdc.md).
