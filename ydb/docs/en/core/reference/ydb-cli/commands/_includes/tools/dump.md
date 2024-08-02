# Creating a data dump

Using the `tools dump` subcommand, you can save a snapshot of a specific table, database schema, or database folder with all nested objects locally.

General command format:

```bash
{{ ydb-cli }} [global options...] tools dump [options...]
```

* `global options`: [Global parameters](../../../commands/global-options.md).
* `options`: [Subcommand parameters](#options).

View a description of the command to create a dump:

```bash
{{ ydb-cli }} tools dump --help
```

## Subcommand parameters {#options}

| Parameter name | Parameter description |
| --- | --- |
| `-p <value>`<br/>`--path <value>` | The path to the folder or table to create a dump of.<br/>By default, a full DB dump will be made. |
| `-o <value>`<br/>`--output <value>` | Required parameter.<br/>The path on the local file system where the dump objects will be placed.<br/>The dump folder must not exist or must be empty. |
| `--scheme-only` | Make a dump of the DB schema only. |
| `--avoid-copy` | Don't make a copy.<br/>By default, to reduce the impact on the user load and for data consistency, {{ ydb-short-name }} first makes a copy of the table and then dumps the copy. In some cases, for example, when dumping tables with external blobs, this approach isn't supported. |
| `--save-partial-result` | Save the results of an incomplete dump.<br/>When this option is selected, all the data written before the operation was interrupted is saved. |
| `--consistency-level <value>` | Consistency level.<br/>A data dump with a consistency level at the DB level takes longer and is more likely to impact the user load. Possible values:<br/><ul><li>`database`: Consistency at the DB level.</li><li>`table`: Consistency at the table level.</li></ul>The default value is `database`. |

## Examples {#examples}

### Full DB dump

Create a dump of the `seasons` table in the local folder `~/dump` with consistency at the table level:

```bash
{{ ydb-cli }} tools dump \
  --path seasons \
  --output ~/dump \
  --consistency-level table
```

View a list of objects in the `dump` folder:

```bash
ls -R ~/dump
```

Result:

```text
/home/user/dump:
episodes  my-directory  seasons  series

/home/user/dump/episodes:
data_00.csv  scheme.pb

/home/user/dump/my-directory:
sub-directory1  sub-directory2

/home/user/dump/my-directory/sub-directory1:
sub-directory1-1

/home/user/dump/my-directory/sub-directory1/sub-directory1-1:

/home/user/dump/my-directory/sub-directory2:

/home/user/dump/seasons:
data_00.csv  scheme.pb

/home/user/dump/series:
data_00.csv  scheme.pb
```
