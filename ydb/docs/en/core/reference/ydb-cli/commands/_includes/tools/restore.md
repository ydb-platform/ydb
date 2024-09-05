# Restoring data from a dump

Using the `tools restore` subcommand, you can restore data from a previously created dump.

General command format:

```bash
{{ ydb-cli }} [global options...] tools restore [options...]
```

* `global options`: [Global parameters](../../../commands/global-options.md).
* `options`: [Subcommand parameters](#options).

View a description of the command to restore data from a dump:

```bash
{{ ydb-cli }} tools restore --help
```

## Parameters of the subcommand {#options}

Parameter name | Parameter description
--- | ---
`-p`<br/>`--path` | The path to the folder or table to dump.<br/>Default value is `.`, a full database dump will be performed.
`-o`<br/>`--output` | Required parameter.<br/>The path on the local file system where the dump objects will be placed. The dump folder must not exist or must be empty.
`--scheme-only` | Make a dump of the DB schema only. Possible values:<br/><ul><li>`0`: No.</li><li>`1`: Yes.</li>Default value is `0`.
`--avoid-copy` | Avoid copying. Possible values:<br/><ul><li>`0`: No.</li><li>`1`: Yes.</li>Default value is `0`.
`--save-partial-result` | Save the results of an incomplete dump. Possible values:<br/><ul><li>`0`: No.</li><li>`1`: Yes.</li>Default value is `0`.
`--consistency-level` | Consistency level. Possible values:<br/><ul><li>`database`: Consistency at the DB level.</li><li>`table`: Consistency at the table level.</li>Default value is `database`.

`-p <value>`<br/>`--path <value>` | Required parameter.<br/>The path in the DB by which the folder or table is restored.
`-o <value>`<br/>`--output <value>` | Required parameter.<br/>The path on the local file system where the dump objects are located.
`--dry-run` | Do not restore tables. Make sure that:<br/><ul><li>— All dump tables are present in the DB.</li><li>— Schemas of all dump tables correspond to schemas of DB tables.
--restore-data VAL       Whether to restore data or not (default: 1)
--restore-indexes VAL    Whether to restore indexes or not (default: 1)
--skip-document-tables VAL
Document API tables cannot be restored for now. Specify this option to skip such tables
(default: 0)
--save-partial-result    Do not remove partial restore result.
Default: 0.
--bandwidth VAL          Limit data upload bandwidth, bytes per second (example: 2MiB) (default: 0)
--rps VAL                Limit requests per second (example: 100) (default: 30)
--upload-batch-rows VAL  Limit upload batch size in rows (example: 1K) (default: 0)
--upload-batch-bytes VAL Limit upload batch size in bytes (example: 1MiB) (default: "512KiB")
--upload-batch-rus VAL   Limit upload batch size in request units (example: 100) (default: 30)
--bulk-upsert            Use BulkUpsert to upload data in more efficient way (default: 0)
--import-data            Use ImportData to upload data (default: 0)

## Examples {#examples}

### Full DB dump

Dump a DB:

```bash
{{ ydb-cli }} tools dump -o ~/dump
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
