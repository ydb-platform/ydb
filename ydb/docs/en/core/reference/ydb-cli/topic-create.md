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
| `--partitions-count VAL` | The number of topic [partitions](../../concepts/topic.md#partitioning).<br>The default value is `1`. |
| `--retention-period-hours VAL` | Data retention time in a topic, set in hours.<br>The default value is `18`. |
| `--supported-codecs STRING` | Supported data compression methods.<br>The default value is `raw,zstd,gzip,lzop`.<br>Possible values:<ul><li>`RAW`: Without compression.</li><li>`ZSTD`: [zstd](https://ru.wikipedia.org/wiki/Zstandard) compression.</li><li>`GZIP`: [gzip](https://ru.wikipedia.org/wiki/Gzip) compression.</li><li>`LZOP`: [lzop](https://ru.wikipedia.org/wiki/Lzop) compression.</li></ul> |

## Examples {examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

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
