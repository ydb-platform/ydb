# Importing data from an S3 compatible storage

The `import s3` command starts, on the server side, the process of importing data and schema object details from an S3-compatible storage, in the format described in the [File structure](../file-structure.md) section:

```bash
{{ ydb-cli }} [connection options] import s3 [options]
```

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

As opposed to the [`tools restore` command](../tools-restore.md), the `import s3` command always creates objects in entirety, so none of the imported objects (directories or tables) should already exist.

If you need to import some more data to your existing S3 tables (for example, using [S3cmd](https://s3tools.org/s3cmd)), you can copy the S3 contents to the file system and use the [`tools restore`](../tools-restore.md) command.

## Command line parameters {#pars}

`[options]`: Command parameters:

### S3 connection parameters {#s3-conn}

To run the command to import data from an S3 storage, specify the [S3 connection parameters](../auth-s3.md). As data is imported by the YDB server asynchronously, the specified endpoint must be available so that a connection can be established from the server side.

### List of imported objects {#items}

`--item STRING`: Description of the item to import. You can specify the `--item` parameter multiple times if you need to import multiple items. `STRING` is set in `<property>=<value>,...` format with the following mandatory properties:
- `source`, `src` or `s` is the path (key prefix) in S3 that hosts the imported directory or table
- `destination`, `dst`, or `d` is the database path to host the imported directory or table. The destination of the path must not exist. All the directories along the path will be created if missing.

### Additional parameters {#aux}

`--description STRING`: A text description of the operation saved in the operation history
`--retries NUM`: The number of import retries to be made by the server. The default value is 10.
`--format STRING`: The format of the results.
- `pretty`: Human-readable format (default).
- `proto-json-base64`: Protobuf in JSON format, binary strings are Base64-encoded.

## Importing {#exec}

### Export result {#result}

If successful, the `import s3` command prints summary information about the enqueued operation to import data from S3 in the format specified in the `--format` option. The import itself is performed by the server asynchronously. The summary shows the operation ID that you can use later to check the operation status and perform actions on it:

- In the default `pretty` mode, the operation ID is displayed in the id field with semigraphics formatting:

   ```
   ┌───────────────────────────────────────────┬───────┬─────...
   | id                                        | ready | stat...
   ├───────────────────────────────────────────┼───────┼─────...
   | ydb://import/8?id=281474976788395&kind=s3 | true  | SUCC...
   ├╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴...
   | Items:
   ...
   ```

- In the proto-json-base64 mode, the operation ID is in the "id" attribute:

   ```
   {"id":"ydb://export/8?id=281474976788395&kind=s3","ready":true, ... }
   ```

### Import status {#status}

Data is imported in the background. To get information on import status, use the `operation get` command with the operation ID **enclosed in quotation marks** and passed as a command parameter. For example:

```bash
{{ ydb-cli }} -p quickstart operation get "ydb://import/8?id=281474976788395&kind=s3"
```

The `operation get` format is also set by the `--format` option.

Although the operation ID is in URL format, there is no guarantee that it is maintained in the future. It should only be interpreted as a string.

You can track the import by changes in the "progress" attribute:

- In the default `pretty` mode, successfully completed export operations are displayed as "Done" in the `progress` field with semigraphics formatting:

   ```
   ┌───── ... ──┬───────┬─────────┬──────────┬─...
   | id         | ready | status  | progress | ...
   ├──────... ──┼───────┼─────────┼──────────┼─...
   | ydb:/...   | true  | SUCCESS | Done     | ...
   ├╴╴╴╴╴ ... ╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴┴╴...
   ...
   ```

- In the proto-json-base64 mode, the completed export operation is indicated with the `PROGRESS_DONE` value of the `progress` attribute:

   ```
   {"id":"ydb://...", ...,"progress":"PROGRESS_DONE",... }
   ```

### Completing the import operation {#forget}

When the import is complete, use `operation forget` to delete the import from the operation list:

```bash
{{ ydb-cli }} -p quickstart operation forget "ydb://import/8?id=281474976788395&kind=s3"
```

### List of import operations {#list}

To get a list of import operations, run the `operation list import/s3` command:

```bash
{{ ydb-cli }} -p quickstart operation list import/s3
```

The `operation list` format is also set by the `--format` option.

## Examples {#examples}

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

### Importing to the database root {#example-full-db}

Importing to the database root the contents of the `export1` directory in the `mybucket` bucket using the S3 authentication parameters taken from the environment variables or the `~/.aws/credentials` file:

```
ydb -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --item src=export1,dst=.
```

### Importing multiple directories {#example-specific-dirs}

Importing items from the dir1 and dir2 directories in the `mybucket` S3 bucket to the same-name database directories using explicitly specified S3 authentication parameters:

```
ydb -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key VJGSOScgs-5kDGeo2hO9 --secret-key fZ_VB1Wi5-fdKSqH6074a7w0J4X0 \
  --item src=export/dir1,dst=dir1 --item src=export/dir2,dst=dir2
```

### Getting operation IDs {#example-list-oneline}

To get a list of import operation IDs in a bash-friendly format, use the [jq](https://stedolan.github.io/jq/download/) utility:

```bash
{{ ydb-cli }} -p quickstart operation list import/s3 --format proto-json-base64 | jq -r ".operations[].id"
```

You'll get a result where each new line shows an operation's ID. For example:

```
ydb://import/8?id=281474976789577&kind=s3
ydb://import/8?id=281474976789526&kind=s3
ydb://import/8?id=281474976788779&kind=s3
```

You can use these IDs, for example, to run a loop to end all the current operations:

```bash
{{ ydb-cli }} -p quickstart operation list import/s3 --format proto-json-base64 | jq -r ".operations[].id" | while read line; do {{ ydb-cli }} -p quickstart operation forget $line;done
```

