# Saving a consumer offset

Each topic consumer has a [consumer offset](../../concepts/topic.md#consumer-offset).

You can use the `topic consumer offset commit` command to save the consumer offset for the consumer that you [added](topic-consumer-add.md).

General format of the command:

```bash
{{ ydb-cli }} [global options...] topic consumer offset commit [options...] <topic-path>
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).
* `topic-path`: Topic path.

Viewing the command description:

```bash
{{ ydb-cli }} topic consumer offset commit --help
```

## Parameters of the subcommand {#options}

| Name | Description |
---|---
| `--consumer <value>` | Consumer name. |
| `--partition <value>` | Partition number. |
| `--offset <value>` | Offset value that you want to set. |

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

For `my-consumer`, set the offset of 123456789 in `my-topic` and partition `1`:

```bash
{{ ydb-cli }} -p db1 topic consumer offset commit \
  --consumer my-consumer \
  --partition 1 \
  --offset 123456789 \
  my-topic
```
