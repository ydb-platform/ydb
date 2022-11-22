# table attribute add

With the `table attribute add` command, you can add a [custom attribute](../../concepts/datamodel/table.md#users-attr) to your table.

General format of the command:

```bash
{{ ydb-cli }} [global options...] table attribute add [options...] <table path>
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Parameters of the subcommand](#options).
* `table path`: The table path.

Look up the description of the command to add a custom attribute:

```bash
{{ ydb-cli }} table attribute add --help
```

## Parameters of the subcommand {#options}

| Name | Description |
---|---
| `--attribute` | The custom attribute in the `<key>=<value>` format. You can use `--attribute` many times to add multiple attributes by a single command. |

## Examples {examples}

Add the custom attributes with the keys `attr_key1`, `attr_key2` and the respective values `attr_value1`, `attr_value2` to the `my-table` table:

```bash
{{ ydb-cli }} table attribute add --attribute attr_key1=attr_value1 --attribute attr_key2=attr_value2 my-table
```