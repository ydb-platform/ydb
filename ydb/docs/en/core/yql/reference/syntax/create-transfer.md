# CREATE TRANSFER

Creates a [transfer](../../../concepts/transfer.md) from a [topic](../../../concepts/datamodel/topic.md) to a [table](../../../concepts/datamodel/table.md).

Syntax:

```yql
CREATE TRANSFER transfer_name 
FROM topic_name TO table_name USING lambda
WITH (option = value[, ...])
```

* `transfer_name` — the name of the transfer to be created.
* `topic_name` — the name of the topic containing the source messages for transformation and writing to the table.
* `table_name` — the name of the table where the data will be written.
* `lambda` — [lambda-function](#lambda) for message transformation. <!-- markdownlint-disable-line MD051 -->
* `option` — a command option:

  * `CONNECTION_STRING` — [the connection string](../../../concepts/connect.md#connection_string) to the database containing the topic. This is only specified if the topic is located in a different {{ ydb-short-name }} database.
  * Authentication settings for the topic's database (required if the topic is in another database), using one of the following methods:

    {% include [x](_includes/async_replication_authentification.md) %}

  * `CONSUMER` — the name of the source topic [consumer](../../../concepts/datamodel/topic.md#consumer). If a name is specified, a consumer with that name must already [exist](alter-topic.md#add-consumer) in the topic, and the transfer will start processing messages from the first uncommitted message in the topic. If no name is specified, a consumer will be added to the topic automatically, and the transfer will start processing messages from the first message stored in the topic. The name of the automatically created consumer can be obtained from the [description](../../../reference/ydb-cli/commands/scheme-describe.md) of the transfer instance.

  * {% include [x](../_includes/transfer_flush.md) %}

## Permissions

The following [permissions](grant.md#permissions-list) are required to create a transfer:

* `CREATE TABLE` — to create a transfer instance;
* `ALTER SCHEMA` — to automatically create a topic consumer (if applicable);
* `SELECT ROW` — to read messages from the source topic;
* `UPDATE ROW` — to update rows in the destination table.

## Examples {#examples}

Creating a transfer instance from the `example_topic` topic to the `example_table` table in the current database:

```yql
CREATE TABLE example_table (
    partition Uint32 NOT NULL,
    offset Uint64 NOT NULL,
    message Utf8,
    PRIMARY KEY (partition, offset)
);

CREATE TOPIC example_topic;

$transformation_lambda = ($msg) -> {
    return [
        <|
            partition: $msg._partition,
            offset: $msg._offset,
            message: CAST($msg._data AS Utf8)
        |>
    ];
};

CREATE TRANSFER example_transfer
    FROM example_topic TO example_table USING $transformation_lambda;

```

Creating a transfer instance from the `example_topic` topic in the `/Root/another_database` database to the `example_table` table in the current database. Before creating the transfer, you need to create the destination table in the current database and create the source topic in the `/Root/another_database` database:

{% include [x](../_includes/secret_tip.md) %}

```yql
$transformation_lambda = ($msg) -> {
    return [
        <|
            partition: $msg._partition,
            offset: $msg._offset,
            message: CAST($msg._data AS Utf8)
        |>
    ];
};

CREATE TRANSFER example_transfer
    FROM example_topic TO example_table USING $transformation_lambda
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret'
);
```

Creating a transfer instance and explicitly specifying the consumer `existing_consumer_of_topic`:

```yql
$transformation_lambda = ($msg) -> {
    return [
        <|
            partition: $msg._partition,
            offset: $msg._offset,
            message: CAST($msg._data AS Utf8)
        |>
    ];
};
CREATE TRANSFER example_transfer
    FROM example_topic TO example_table USING $transformation_lambda
WITH (
    CONSUMER = 'existing_consumer_of_topic'
);
```

Example of processing a message in JSON format:

```yql
// example message:
// {
//   "update": {
//     "operation":"value_1"
//   },
//   "key": [
//     "id_1",
//     "2019-01-01T15:30:00.000000Z"
//   ]
// }

$transformation_lambda = ($msg) -> {
    $json = CAST($msg._data AS JSON);
    return [
        <|
            timestamp: CAST(Yson::ConvertToString($json.key[1]) AS Timestamp),
            object_id: CAST(Yson::ConvertToString($json.key[0]) AS Utf8),
            operation: CAST(Yson::ConvertToString($json.update.operation) AS Utf8)
        |>
    ];
};

CREATE TRANSFER example_transfer
    FROM example_topic TO example_table USING $transformation_lambda;
```

Creating a transfer instance and explicitly specifying the batching option:

```yql
$transformation_lambda = ($msg) -> {
    return [
        <|
            partition: $msg._partition,
            offset: $msg._offset,
            message: CAST($msg._data AS Utf8)
        |>
    ];
};
CREATE TRANSFER example_transfer
    FROM example_topic TO example_table USING $transformation_lambda
WITH (
    BATCH_SIZE_BYTES = 1048576,
    FLUSH_INTERVAL = Interval('PT60S')
);
```

{% include [x](../_includes/transfer_lambda.md) %}

## See Also

* [ALTER TRANSFER](alter-transfer.md)
* [DROP TRANSFER](drop-transfer.md)
* [{#T}](../../../concepts/transfer.md)
