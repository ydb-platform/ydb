# Transfer — quick start

This guide helps you get started with [transfer](../../concepts/transfer.md) in {{ ydb-short-name }} using a basic example.

This guide covers the following steps for working with transfers:

* [creating a topic](#step1), for the transfer to read from;
* [creating a table](#step2), for the transfer to write data to;
* [creating the transfer](#step3);
* [populating the topic with data](#step4);
* [verifying the table contents](#step5).

## Step 1. Create a topic {#step1}

First, you need to create a [topic](../../concepts/datamodel/topic.md) in {{ ydb-short-name }} that the transfer will read data from. You can do this using a [YQL query](../../yql/reference/syntax/create-topic.md):

```yql
CREATE TOPIC `transfer_recipe/source_topic`;
```

The `transfer_recipe/source_topic` topic lets you transfer any unstructured data.

## Step 2. Create a table {#step2}

After creating the topic, you need to create a [table](../../concepts/datamodel/table.md) that will receive data from the `source_topic` topic. You can do this using a [YQL query](../../yql/reference/syntax/create_table/index.md):

```yql
CREATE TABLE `transfer_recipe/target_table` (
  partition Uint32 NOT NULL,
  offset Uint64 NOT NULL,
  data String,
  PRIMARY KEY (partition, offset)
);
```

The `transfer_recipe/target_table` table has three columns:

* `partition` — the ID of the topic [partition](../../concepts/glossary.md#partition) the message was received from;
* `offset` — [the sequence number](../../concepts/glossary.md#offset) that identifies the message within its partition;
* `data` — the message body.

## Step 3. Create a transfer {#step3}

After creating the topic and the table, you need to create a data [transfer](../../concepts/transfer.md) that will move messages from the topic to the table. You can do this using a [YQL query](../../yql/reference/syntax/create-transfer.md):

```yql
$transformation_lambda = ($msg) -> {
    return [
        <|
            partition: $msg._partition,
            offset: $msg._offset,
            data: $msg._data
        |>
    ];
};

CREATE TRANSFER `transfer_recipe/example_transfer`
  FROM `transfer_recipe/source_topic` TO `transfer_recipe/target_table`
  USING $transformation_lambda;
```

In this example:

*  `$transformation_lambda` - a transformation rule for converting a topic message into table columns. In this case, the topic message is transferred to the table without any changes. To learn more about configuring transformation rules, see the [documentation](../../yql/reference/syntax/create-transfer.md#lambda);
*  `$msg` - a variable that contains the topic message being processed.


## Step 4. Populate the topic with data {#step4}

After creating the transfer, you can write messages to the topic, for example, using the [{{ ydb-short-name }} CLI](../../reference/ydb-cli/index.md).

{% include [x](../../_includes/ydb-cli-profile.md) %}

```bash
echo "Message 1" | ydb --profile quickstart topic write 'transfer_recipe/source_topic'
echo "Message 2" | ydb --profile quickstart topic write 'transfer_recipe/source_topic'
echo "Message 3" | ydb --profile quickstart topic write 'transfer_recipe/source_topic'
```

## Step 5. Verify the table contents {#step5}

After writing messages to the `source_topic` topic, records will appear in the `transfer_recipe/target_table` table after a short delay. You can verify this using a [YQL query](../../yql/reference/syntax/select/index.md):

```yql
SELECT *
FROM `transfer_recipe/target_table`;
```

Query result:

| partition | offset | data |
|-----------|--------|------|
| 0         | 0      | Message 1 |
| 0         | 1      | Message 2 |
| 0         | 2      | Message 3 |

{% include [x](_includes/batching.md) %}

## Conclusion

This guide provides a basic example of working with a transfer: creating a topic, table, and transfer, writing data to the topic, and verifying the result.

These examples are designed to illustrate the syntax for working with transfers. For a more realistic example, see the [article](nginx.md) that describes how to stream NGINX access logs.

## See Also

* [{#T}](../../concepts/transfer.md)
* [{#T}](nginx.md)
