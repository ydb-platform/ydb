# Copying a table

Using the `tools copy` subcommand, you can create a copy of a DB table.

General command format:

```bash
{{ ydb-cli }} [global options...] tools copy [options...]
```

* `global options`: [Global parameters](../../../commands/global-options.md).
* `options`: [Subcommand parameters](#options).

View a description of the command to copy a table:

```bash
{{ ydb-cli }} tools copy --help
```

## Parameters of the subcommand {#options}

| Parameter name | Parameter description |
| --- | --- |
| `--client-timeout <value>` | Client side operation timeout, ms. |
| `--operation-timeout <value>` | Server side operation timeout, ms. |
| `--cancel-after <value>` | Operation lifetime, after which the operation is canceled, ms. |
| `--item <value>=<value>,...` | Operation parameters. Possible values:<br/><ul><li>`destination`, `dst`, or `d` — required parameter, the path of the destination table. If the destination path contains folders, they must be created in advance. The table with the destination name should not exist.</li><li>`source`, `src`, or `s` — required parameter, the path of the source table.</li></ul> |

## Examples {#examples}

### Copying a table {#copy-table}

Create the `backup` folder in the DB:

```bash
{{ ydb-cli }} scheme mkdir backup
```

Copy the `seasons` table to the `seasons-v1` table in the `backup` folder:

```bash
{{ ydb-cli }} tools copy \
  --item destination=backup/seasons-v1,source=seasons
```

View the listing of objects in the `backup` folder:

```bash
{{ ydb-cli }} scheme ls backup
```

Result:

```text
seasons-v1
```
