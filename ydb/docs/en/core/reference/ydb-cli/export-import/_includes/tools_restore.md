# Importing data from the file system

The `tools restore` command creates the items of the database schema in the database, and populates them with the data previously exported there with the `tools dump` command or prepared manually as per the rules from the [File structure](../file-structure.md) article:

```bash
{{ ydb-cli }} [connection options] tools restore -p PATH -i PATH [options]
```

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

If the table or directory already exists in the database, no changes will be made to its schema and ACL. If some columns present in the imported files are missing in the database or have mismatching types, this may lead to the data import operation failing.

To import data to the table, use the [YQL `REPLACE` command](../../../../yql/reference/syntax/replace_into.md). If the table included any records before the import, the records whose keys are present in the imported files are replaced by the data from the file. The records whose keys are absent in the imported files aren't affected.

## Required parameters {#mandatory}

`-p PATH` or `--path PATH`: Path to the database directory the data will be imported to. To import data to the root directory, specify `.`. All the missing directories along the path will be created.

`-i PATH` or `--input PATH`: Path to the directory in the client system the data will be imported from.

## Optional parameters {#optional}

`[options]`: Optional parameters of the command:

`--restore-data VAL`: Enables/disables data import, 1 (yes) or 0 (no), defaults to 1. If set to 0, the import only creates items in the schema without populating them with data. If there's no data in the file system (only the schema has been exported), it doesn't make sense to change this option.

`--restore-indexes VAL`: Enables/disables import of indexes, 1 (yes) or 0 (no), defaults to 1. If set to 0, the import won't either register indexes in the data schema or populate them with data.

`--restore-acl VAL`: Enables/disables import of ACL, 1 (yes) or 0 (no), defaults to 1. If set to 0, the import creates items in the schema with an empty ACL, and their owner will be the user who started the import.

`--dry-run`: Matching the data schemas in the database and file system without updating the database, 1 (yes) or 0 (no), defaults to 0. When enabled, the system checks that:

- All tables in the file system are present in the database
- These items are based on the same schema, both in the file system and in the database

`--save-partial-result`: Save the partial import result. If disabled, an import error results in reverting to the database state before the import.

`--import-data`: Use ImportData – a more efficient way to upload data than the default one. This method sends data to the server partitioned by client and in a lighter format. However, it will return an error when attempting to import the exported data into an existing table that already has indexes or is in the process of building them. Therefore, if you need to restore a table with indexes, make sure they're not already present in the scheme (for example, using the [`ydb scheme ls`](https://ydb.tech/docs/en/reference/ydb-cli/commands/scheme-ls) command). By default ImportData is disabled.

### Workload restriction parameters {#limiters}

Using the below parameters, you can limit the import workload against the database.

{% note warning "Attention!" %}

Some of the below parameters have default values. This means that the workload will be limited even if none of them is mentioned in `tools restore`.

{% endnote %}

`--rps VAL`: Limits the number of queries used to upload batches to the database per second, the default value is 30.
`--bandwidth VAL`: Limit the workload per second, defaults to 0 (not set). `VAL` specifies the data amount with a unit, for example, 2MiB. If this value is set, the `--rps` limit (see above) is not applied.
`--in-flight VAL`: Limits the number of queries that can be run in parallel, the default value is 10. To achieve maximum parallelism, set the parameter value to the number of cores allocated for the restore process.
`--upload-batch-rows VAL`: Limits the number of records in the uploaded batch, the default value is 0 (unlimited). `VAL` determines the number of records and is set as a number with an optional unit, for example, 1K.
`--upload-batch-bytes VAL`: Limits the batch size of uploaded data, the default value is 512KB. `VAL` specifies the data amount with a unit, for example, 1MiB. Maximum value is 16 MiB.
`--upload-batch-rus VAL`: Applies only to Serverless databases to limit Request Units (RU) that can be consumed to upload one batch, defaults to 30 RU. The batch size is selected to match the specified value. `VAL` determines the number of RU and is set as a number with an optional unit, for example, 100 or 1K.

## Examples {#examples}

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

### Importing to the database root

From the current file system directory:

```bash
{{ ydb-cli }} -p quickstart tools restore -p . -i .
```

From the current file system directory:

```bash
{{ ydb-cli }} -p quickstart tools restore -p . -i ~/backup_quickstart
```

### Uploading data to the specified directory in the database

From the current file system directory:

```bash
{{ ydb-cli }} -p quickstart tools restore -p dir1/dir2 -i .
```

From the current file system directory:

```bash
{{ ydb-cli }} -p quickstart tools restore -p dir1/dir2 -i ~/backup_quickstart
```

Matching schemas between the database and file system:

```bash
{{ ydb-cli }} -p quickstart tools restore -p dir1/dir2 -i ~/backup_quickstart --dry-run
```

### Example options for better performance

```bash
{{ ydb-cli }} -p quickstart tools restore -p . -i . --import-data --bandwidth=10GiB --in-flight=16 --upload-batch-bytes=16MiB
```
