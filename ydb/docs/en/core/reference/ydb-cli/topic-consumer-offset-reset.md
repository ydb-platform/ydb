# Resetting a consumer offset

Each topic consumer has a [consumer offset](../../concepts/datamodel/topic.md#consumer-offset).

You can use the `topic consumer offset reset` command to reset the committed offset for the consumer that you [added](topic-consumer-add.md) on **all** partitions of the topic, including inactive partitions after a split or merge.

The command is supported starting from {{ ydb-short-name }} server version **27.1**.

General format of the command:

```bash
{{ ydb-cli }} [global options...] topic consumer offset reset [options...] <topic-path>
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).
* `topic-path`: Topic path.

Viewing the command description:

```bash
{{ ydb-cli }} topic consumer offset reset --help
```

On success the command prints `OK`. If the offset cannot be reset on some partitions, the command prints issues with the list of failed partition identifiers.

## Parameters of the subcommand {#options}

| Name | Description |
|------|-------------|
| `--consumer <value>` | Consumer name. |
| `--position <value>` | Target position: `earliest`, `latest`, or a timestamp. The timestamp may be specified in unix time format (seconds from 1970.01.01) or in ISO-8601 format (like `2020-07-10T15:00:00Z`). |

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Reset `my-consumer` to the beginning of `my-topic`:

```bash
{{ ydb-cli }} -p db1 topic consumer offset reset \
  --consumer my-consumer \
  --position earliest \
  my-topic
```
