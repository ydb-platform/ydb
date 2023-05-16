# Deleting a topic

You can use the `topic drop` subcommand to delete a [previously created](topic-create.md) topic.

{% note info %}

Deleting a topic also deletes all the consumers added for it.

{% endnote %}

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

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

Delete the [previously created](topic-create.md) topic:

```bash
{{ ydb-cli }} -p quickstart topic drop my-topic
```
