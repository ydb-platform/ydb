# table attribute drop

With the `table attribute drop` command, you can drop a [custom attribute ](../../concepts/datamodel/table.md#users-attr) from your table.

General format of the command:

```bash
{{ ydb-cli }} [global options...] table attribute drop [options...] <table path>
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).
* `table path`: The table path.

Look up the description of the command to add a custom attribute:

```bash
{{ ydb-cli }} table attribute drop --help
```

## Parameters of the subcommand {#options}

| Name | Description |
---|---
| `--attributes` | The key of the custom attribute to be dropped. You can list multiple keys separated by a comma (`,`). |

## Examples {examples}

Drop the custom attributes with the keys `attr_key1` and `attr_key2` from the `my-table` table:

```bash
{{ ydb-cli }} table attribute drop --attributes attr_key1,attr_key2 my-table
```