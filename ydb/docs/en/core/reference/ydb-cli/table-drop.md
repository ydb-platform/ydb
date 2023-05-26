# Deleting a table

Using the `table drop` subcommand, you can delete a specified table.

General format of the command:

```bash
{{ ydb-cli }} [global options...] table drop [options...] <table path>
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).
* `table path`: The table path.

To view a description of the table delete command:

```bash
{{ ydb-cli }} table drop --help
```

## Parameters of the subcommand {#options}

| Name | Description |
---|---
| `--timeout` | The time within which the operation should be completed on the server. |

## Examples {examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

To delete the table `series`:

```bash
{{ ydb-cli }} -p quickstart table drop series
```
