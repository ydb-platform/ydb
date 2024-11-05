# Creating a topic

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

## Parameters of the subcommand {#options}

| Name | Description |
---|---
| `--partitions-count` | The number of topic [partitions](../../concepts/topic.md#partitioning).<br/>The default value is `1`. |
| `--retention-period-hours` | Data retention time in a topic, set in hours.<br/>The default value is `18`. |
| `--partition-write-speed-kbps` | The maximum write speed to a [partition](../../concepts/topic.md#partitioning), specified in KB/s.<br/>The default value is `1024`. |
| `--retention-storage-mb` | The maximum storage size, specified in MB. When the limit is reached, the oldest data will be deleted.<br/>The default value is `0` (no limit). |
| `--supported-codecs` | Supported data compression methods. Set with a comma.<br/>The default value is `raw`.<br/>Possible values:<ul><li>`RAW`: No compression.</li><li>`ZSTD`: [zstd](https://en.wikipedia.org/wiki/Zstandard) compression.</li><li>`GZIP`: [gzip](https://en.wikipedia.org/wiki/Gzip) compression.</li><li>`LZOP`: [lzop](https://en.wikipedia.org/wiki/Lzop) compression.</li></ul> |
| `--metering-mode` | The topic pricing method for a serverless database.<br/>Possible values:<ul><li>`request-units`: Based on actual usage.</li><li>`reserved-capacity`: Based on dedicated resources.</li></ul> |

## Examples {examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Create a topic with 2 partitions, `RAW` and `GZIP` compression methods, message retention time of 2 hours, and the `my-topic` path:

```bash
{{ ydb-cli }} -p quickstart topic create \
  --partitions-count 2 \
  --supported-codecs raw,gzip \
  --retention-period-hours 2 \
  my-topic
```

View parameters of the created topic:

```bash
{{ ydb-cli }} -p quickstart scheme describe my-topic
```

Result:

```text
RetentionPeriod: 2 hours
PartitionsCount: 2
SupportedCodecs: RAW, GZIP
```
