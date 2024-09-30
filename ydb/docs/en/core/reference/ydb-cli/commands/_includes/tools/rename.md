# Renaming a table

Using the `tools rename` subcommand, you can [rename](../../../../../concepts/datamodel/table.md#rename) one or more tables at the same time, move a table to another directory within the same database, replace one table with another one within the same transaction.

General command format:

```bash
{{ ydb-cli }} [global options...] tools rename [options...]
```

* `global options`: [Global parameters](../../../commands/global-options.md).
* `options`: [Subcommand parameters](#options).

View a description of the command to rename a table:

```bash
{{ ydb-cli }} tools rename --help
```

## Subcommand parameters {#options}

A single run of the `tools rename` command executes a single rename transaction that may include one or more operations to rename different tables.

| Parameter name | Parameter description |
| --- | --- |
| `--item <property>=<value>,...` | Description of the rename operation. Can be specified multiple times if multiple rename operations need to be executed within a single transaction.<br/><br/>Required properties:<br/><ul><li>`source`, `src`, and `s`: Path to the source table.</li><li>`destination`, `dst`, and `d`: Path to the destination table. If the destination path contains folders, they must be [created in advance](../../dir.md#mkdir).</li></ul>Advanced properties:<br/><ul> <li>`replace`, `force`: Overwrite the destination table. If `True`, the destination table is overwritten with its data deleted. `False`: If the destination table exists, an error is returned and the entire rename transaction is canceled. Default value: `False`.</li></ul> |
| `--timeout <value>` | Operation timeout, ms. |

When including multiple rename operations in a single `tools rename` call, they're executed in the specified order, but within a single transaction. This lets you rotate the table under load without data loss: the first operation is renaming the working table to the backup one and the second is renaming the new table to the working one.

## Examples {#examples}

- Renaming a single table:

  ```bash
  {{ ydb-cli }} tools rename --item src=old_name,dst=new_name
  ```

- Renaming multiple tables within a single transaction:

  ```bash
  {{ ydb-cli }} tools rename \
    --item source=new-project/main_table,destination=new-project/episodes \
    --item source=new-project/second_table,destination=new-project/seasons \
    --item source=new-project/third_table,destination=new-project/series
  ```

- Moving tables to a different directory:

  ```bash
  {{ ydb-cli }} tools rename \
    --item source=new-project/main_table,destination=cinema/main_table \
    --item source=new-project/second_table,destination=cinema/second_table \
    --item source=new-project/third_table,destination=cinema/third_table
  ```

- Replacing a table

  ```bash
  {{ ydb-cli }} tools rename \
    --item replace=True,source=pre-prod-project/main_table,destination=prod-project/main_table
  ```

- Rotating a table

  ```bash
  {{ ydb-cli }} tools rename \
    --item source=prod-project/main_table,destination=prod-project/main_table.backup \
    --item source=pre-prod-project/main_table,destination=prod-project/main_table
  ```
