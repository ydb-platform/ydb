# Updating a topic

You can use the `topic alter` subcommand to update a [previously created](topic-create.md) topic.

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

## Parameters of the subcommand {#options}

The command changes the values of parameters specified in the command line. The other parameter values remain unchanged.

| Name | Description |
---|---
| `--partitions-count` | The number of topic [partitions](../../concepts/topic.md#partitioning). You can only increase the number of partitions. |
| `--retention-period-hours` | The retention period for topic data, in hours. |
| `--partition-write-speed-kbps` | The maximum write speed to a [partition](../../concepts/topic.md#partitioning), specified in KB/s.<br/>The default value is `1024`. |
| `--retention-storage-mb` | The maximum storage size, specified in MB. When the limit is reached, the oldest data will be deleted.<br/>The default value is `0` (no limit). |
| `--supported-codecs` | Supported data compression methods.<br/>Possible values:<ul><li>`RAW`: No compression.</li><li>`ZSTD`: [zstd](https://en.wikipedia.org/wiki/Zstandard) compression.</li><li>`GZIP`: [gzip](https://en.wikipedia.org/wiki/Gzip) compression.</li><li>`LZOP`: [lzop](https://en.wikipedia.org/wiki/Lzop) compression.</li></ul> |
| `--metering-mode` | The topic pricing method for a serverless database.<br/>Possible values:<ul><li>`request-units`: Based on actual usage.</li><li>`reserved-capacity`: Based on dedicated resources.</li></ul> |

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Add a partition and the `lzop` compression method to the [previously created](topic-create.md) topic:

```bash
{{ ydb-cli }} -p quickstart topic alter \
  --partitions-count 3 \
  --supported-codecs raw,gzip,lzop \
  my-topic
```

Make sure that the topic parameters have been updated:

```bash
{{ ydb-cli }} -p quickstart scheme describe my-topic
```

Result:

```text
RetentionPeriod: 2 hours
PartitionsCount: 3
SupportedCodecs: RAW, GZIP, LZOP
```
