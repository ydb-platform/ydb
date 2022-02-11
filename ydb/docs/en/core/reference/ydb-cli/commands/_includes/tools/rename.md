# Renaming a table

Using the `tools rename` subcommand, you can rename one or more tables at the same time, move a table to a different directory within the same database, or replace one table with another within the same transaction.

General command format:

```bash
ydb [global options...] tools rename [options...]
```

* `global options`: [Global parameters](../../../commands/global-options.md).
* `options`: [Subcommand parameters](#options).

View a description of the command to rename a table:

```bash
ydb tools rename --help
```

## Subcommand parameters {#options}

| Parameter name | Parameter description |
| --- | --- |
| `--timeout <value>` | Operation timeout, ms. |
| `--item <value>=<value>,...` | Operation parameters. Possible values:<br/><ul><li>`destination`, `dst`, or `d`: Required parameter, the path to the destination table. If the destination path contains directories, they must be created in advance.</li> <li>`source`, `src`, or `s`: Required parameter, the path of the source table.</li><li>`replace`, `force`: Optional parameter. If the value is `True`, the destination table must exist and will be overwritten. `False`: The destination table must not exist. Default value: `False`.</li></ul> |

## Examples {#examples}

### Renaming a table {#rename}

The database contains a `new-project` directory with tables such as `main_table`, `second_table`, and `third_table`.

Rename the tables:

```bash
{{ ydb-cli }} tools rename \
  --item source=new-project/main_table,destination=new-project/episodes \
  --item source=new-project/second_table,destination=new-project/seasons \
  --item source=new-project/third_table,destination=new-project/series
```

Get a listing of objects:

```bash
ydb scheme ls new-project
```

Result:

```text
episodes  seasons  series
```

### Moving tables {#move}

The database contains a `new-project` directory with tables such as `main_table`, `second_table`, and `third_table`.

Create a directory:

```bash
{{ ydb-cli }} scheme mkdir cinema
```

Rename the tables and move them to the created directory:

```bash
{{ ydb-cli }} tools rename \
  --item source=new-project/main_table,destination=cinema/episodes \
  --item source=new-project/second_table,destination=cinema/seasons \
  --item source=new-project/third_table,destination=cinema/series
```

Get a listing of objects in the created directory:

```bash
ydb scheme ls cinema
```

Result:

```text
episodes  seasons  series
```

### Replacing a table {#replace}

The database contains a `prod-project` directory with tables such as `main_table` and `other_table`, and a `pre-prod-project` directory with tables such as `main_table` and `other_table`.

Replace the `main_table` in the `prod-project` directory with the same-name table from the `pre-prod-project` directory:

```bash
{{ ydb-cli }} tools rename \
  --item replace=True,source=pre-prod-project/main_table,destination=prod-project/main_table
```

Get a listing of `prod-project` directory objects:

```bash
ydb scheme ls prod-project
```

Result:

```text
main_table  other_table
```

Get a listing of `pre-prod-project` directory objects:

```bash
ydb scheme ls pre-prod-project
```

Result:

```text
other_table
```

The `pre-prod-project` directory no longer contains the `main_table`.

### Replacing a table without deleting it {#switch}

The database contains a `prod-project` directory with tables such as `main_table` and `other_table`, and a `pre-prod-project` directory with tables such as `main_table` and `other_table`.

Move the `prod-project/main_table` to the `prod-project/main_table.backup` and the `pre-prod-project/main_table` to the `prod-project/main_table`:

```bash

{{ ydb-cli }} tools rename \
  --item source=prod-project/main_table,destination=prod-project/main_table.backup \
  --item source=pre-prod-project/main_table,destination=prod-project/main_table
```

Get a listing of `prod-project` directory objects:

```bash
ydb scheme ls prod-project
```

Result:

```text
main_table  other_table main_table.backup
```

Get a listing of `pre-prod-project` directory objects:

```bash
ydb scheme ls pre-prod-project
```

Result:

```text
other_table
```

The `pre-prod-project` directory no longer contains the `main_table`.

