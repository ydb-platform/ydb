# Deleting a topic consumer

You can use the `topic consumer drop` command to delete a [previously added](topic-consumer-add.md) consumer.

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

## Parameters of the subcommand {#options}

| Name | Description |
---|---
| `--consumer VAL` | Name of the consumer to be deleted. |

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Delete the [previously created](#consumer-add) consumer with the `my-consumer` name for the `my-topic` topic:

```bash
{{ ydb-cli }} -p quickstart topic consumer drop \
  --consumer my-consumer \
  my-topic
```
