# Working with topics

You can use the `topic` subcommand to create, update, or delete a [topic](../../concepts/topic.md) as well as to create or delete a [consumer](../../concepts/topic.md#consumer).

The examples use the `db1` profile. To learn more, see [{#T}](../../getting_started/cli.md#profile).

## Creating a topic {#topic-create}

You can use the `topic create` subcommand to create a new topic.

General format of the command:

```bash
{{ ydb-cli }} [global options...] topic create [options...] <topic-path>
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).
* `topic-path`: Topic path.

View the description of the create topic command:

```bash
{{ ydb-cli }} topic create --help
```

### Parameters of the subcommand {#topic-create-options}

| Name | Description |
---|---
| `--partitions-count VAL` | The number of topic [partitions](../../concepts/topic.md#partitioning).<br>The default value is `1`. |
| `--retention-period-hours VAL` | Data retention time in a topic, set in hours.<br>The default value is `18`. |
| `--supported-codecs STRING` | Supported data compression methods.<br>The default value is `raw,zstd,gzip,lzop`.<br>Possible values:<ul><li>`RAW`: Without compression.</li><li>`ZSTD`: [zstd](https://ru.wikipedia.org/wiki/Zstandard) compression.</li><li>`GZIP`: [gzip](https://ru.wikipedia.org/wiki/Gzip) compression.</li><li>`LZOP`: [lzop](https://ru.wikipedia.org/wiki/Lzop) compression.</li></ul> |

### Examples {#topic-create-examples}

Create a topic with 2 partitions, `RAW` and `GZIP` compression methods, message retention time of 2 hours, and the `my-topic` path:

```bash
{{ ydb-cli }} -p db1 topic create \
  --partitions-count 2 \
  --supported-codecs raw,gzip \
  --retention-period-hours 2 \
  my-topic
```

View parameters of the created topic:

```bash
{{ ydb-cli }} -p db1 scheme describe my-topic
```

Result:

```text
RetentionPeriod: 2 hours
PartitionsCount: 2
SupportedCodecs: RAW, GZIP
```

## Updating a topic {#topic-alter}

You can use the `topic alter` subcommand to update a [previously created](#topic-create) topic.

General format of the command:

```bash
{{ ydb-cli }} [global options...] topic alter [options...] <topic-path>
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).
* `topic-path`: Topic path.

View the description of the update topic command:

```bash
{{ ydb-cli }} topic alter --help
```

### Parameters of the subcommand {#topic-alter-options}

| Name | Description |
---|---
| `--partitions-count VAL` | The number of topic [partitions](../../concepts/topic.md#partitioning).<br>The default value is `1`. |
| `--retention-period-hours VAL` | Data retention time in a topic, set in hours.<br>The default value is `18`. |
| `--supported-codecs STRING` | Supported data compression methods.<br>The default value is `raw,zstd,gzip,lzop`.<br>Possible values:<ul><li>`RAW`: Without compression.</li><li>`ZSTD`: [zstd](https://ru.wikipedia.org/wiki/Zstandard) compression.</li><li>`GZIP`: [gzip](https://ru.wikipedia.org/wiki/Gzip) compression.</li><li>`LZOP`: [lzop](https://ru.wikipedia.org/wiki/Lzop) compression.</li></ul> |

### Examples {#topic-alter-examples}

Add a partition and the `lzop` compression method to the [previously created](#topic-create) topic:

```bash
{{ ydb-cli }} -p db1 topic alter \
  --partitions-count 3 \
  --supported-codecs raw,gzip,lzop \
  my-topic
```

Make sure that the topic parameters have been updated:

```bash
{{ ydb-cli }} -p db1 scheme describe my-topic
```

Result:

```text
RetentionPeriod: 2 hours
PartitionsCount: 3
SupportedCodecs: RAW, GZIP, LZOP
```

## Deleting a topic {#topic-drop}

You can use the `topic drop` subcommand to delete a [previously created](#topic-create) topic.

General format of the command:

```bash
{{ ydb-cli }} [global options...] topic drop <topic-path>
```

* `global options`: [Global parameters](commands/global-options.md).
* `topic-path`: Topic path.

View the description of the delete topic command:

```bash
{{ ydb-cli }} topic drop --help
```

### Examples {#topic-drop-examples}

Delete the [previously created](#topic-create) topic:

```bash
{{ ydb-cli }} -p db1 topic drop my-topic
```

## Creating a consumer for a topic {#consumer-add}

You can use the `topic consumer add` subcommand to create a consumer for a [previously created](#topic-create) topic.

General format of the command:

```bash
{{ ydb-cli }} [global options...] topic consumer add [options...] <topic-path>
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).
* `topic-path`: Topic path.

View the description of the add consumer command:

```bash
{{ ydb-cli }} topic consumer add --help
```

### Parameters of the subcommand {#consumer-add-options}

| Name | Description |
---|---
| `--consumer VAL` | Name of the consumer to be created. |
| `--starting-message-timestamp VAL` | Time in [UNIX timestamp](https://en.wikipedia.org/wiki/Unix_time) format. Consumption starts as soon as the first [message](../../concepts/topic.md#message) is received after the specified time. If the time is not specified, consumption will start from the oldest message in the topic. |

### Examples {#consumer-add-examples}

Create a consumer with the `my-consumer` name for the [previously created](#topic-create) `my-topic` topic. Consumption will start as soon as the first message is received after August 15, 2022 13:00:00 GMT:

```bash
{{ ydb-cli }} -p db1 topic consumer add \
  --consumer my-consumer \
  --starting-message-timestamp 1660568400 \
  my-topic
```

Make sure the consumer was created:

```bash
{{ ydb-cli }} -p db1 scheme describe my-topic
```

Result:

```text
RetentionPeriod: 2 hours
PartitionsCount: 2
SupportedCodecs: RAW, GZIP

Consumers:
┌──────────────┬─────────────────┬───────────────────────────────┬───────────┐
| ConsumerName | SupportedCodecs | ReadFrom                      | Important |
├──────────────┼─────────────────┼───────────────────────────────┼───────────┤
| my-consumer  | RAW, GZIP       | Mon, 15 Aug 2022 16:00:00 MSK | 0         |
└──────────────┴─────────────────┴───────────────────────────────┴───────────┘
```

## Deleting a consumer {#consumer-drop}

You can use the `topic consumer drop` subcommand to delete a [previously created](#consumer-add) consumer.

General format of the command:

```bash
{{ ydb-cli }} [global options...] topic consumer drop [options...] <topic-path>
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).
* `topic-path`: Topic path.

View the description of the delete consumer command:

```bash
{{ ydb-cli }} topic consumer drop --help
```

### Parameters of the subcommand {#consumer-drop-options}

| Name | Description |
---|---
| `--consumer VAL` | Name of the consumer to be deleted. |

### Examples {#consumer-drop-examples}

Delete the [previously created](#consumer-add) consumer with the `my-consumer` name for the `my-topic` topic:

```bash
{{ ydb-cli }} -p db1 topic consumer drop \
  --consumer my-consumer \
  my-topic
```
