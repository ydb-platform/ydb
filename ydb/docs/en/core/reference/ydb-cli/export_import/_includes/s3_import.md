# Importing data from S3-compatible storage

Running the `import s3` command starts, on the server side, importing data and information about data schema objects from S3-compatible storage in the format described in the [File structure](../file_structure.md) article:

```bash
{{ ydb-cli }} [connection options] import s3 [options]
```

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

Unlike [`tools restore`](../tools_restore.md), the `import s3` command always creates entire objects, meaning that none of the objects being imported (neither directories nor tables) should exist for the command to run successfully.

If you need to import additional data from S3 to existing tables, you can copy the S3 contents to the file system (for example, using [S3cmd](https://s3tools.org/s3cmd)) and run the [`tools restore`](../tools_restore.md) command.

## Command line parameters {#pars}

`[options]`: Command parameters:

### S3 connection parameters {#s3-conn}

To run the command to import data from S3, make sure to specify the [S3 connection parameters](../s3_conn.md). Since data import is performed asynchronously by the YDB server, the specified endpoint must be available to establish a server-side connection.

### List of imported objects {#items}

`--item STRING`: Description of the object to import. The `--item` parameter can be specified several times if you need to import multiple objects. The `STRING` format is `<property>=<value>,...`, with the following properties required:

- `source`, `src`, or `s`: Path to S3 (key prefix) specifying the directory or table to import.
- `destination`, `dst`, or`d`: Path to the DB that will store the imported directory or table. The final element of the path must not exist. All directories specified in the path will be created if they don't exist.

### Additional parameters {#aux}

`--description STRING`: Operation text description stored in the history of operations. `--retries NUM`: Number of import retries the server will make. Defaults to 10.
`--format STRING`: Result output format.

- `pretty`: Human-readable format (default).
- `proto-json-base64`: Protobuf that supports JSON values encoded as binary strings using base64 encoding.

## Importing data {#exec}

### Import result {#result}

If successful , the `import s3` command outputs summary information about the enqueued operation for importing data from S3 in the format specified in the `--format` option. The actual import operation is performed by the server asynchronously. The summary displays the operation ID that can be used later to check the status and actions with the operation:

- In the `pretty` output mode used by default, the operation identifier is output in the id field with semigraphics formatting:

  ```
  ┌───────────────────────────────────────────┬───────┬─────...
  | id                                        | ready | stat...
  ├───────────────────────────────────────────┼───────┼─────...
  | ydb://import/8?id=281474976788395&kind=s3 | true  | SUCC...
  ├╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴...
  | Items:
  ...                                                   
  ```

- In proto-json-base64 output mode, the ID is in the "id" attribute:

  ```
  {"id":"ydb://export/8?id=281474976788395&kind=s3","ready":true, ... }
  ```

### Import status {#status}

Data is imported in the background. You can get information about the status and progress of the import operation by running the `operation get` command with the **quoted** operation ID passed as the command parameter. For example:

```bash
{{ ydb-cli }} -p db1 operation get "ydb://import/8?id=281474976788395&kind=s3"
```

The format of the `operation get` command output is also specified in the `--format` option.

Although the operation ID format is URL, there is no guarantee that it's retained later. It should only be interpreted as a string.

You can track the completion of the import operation by changes in the "progress" attribute:

- In the `pretty` output mode used by default, a successful operation is indicated by the "Done" value in the `progress` field with semigraphics formatting:

  ```
  ┌───── ... ──┬───────┬─────────┬──────────┬─...
  | id         | ready | status  | progress | ...
  ├──────... ──┼───────┼─────────┼──────────┼─...
  | ydb:/...   | true  | SUCCESS | Done     | ...
  ├╴╴╴╴╴ ... ╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴┴╴...
  ...
  ```

- In proto-json-base64 output mode, a completed operation is indicated by the `PROGRESS_DONE` value of the `progress` attribute:

  ```
  {"id":"ydb://...", ...,"progress":"PROGRESS_DONE",... }
  ```

### Ending the import operation {#forget}

Once the data is imported, use the `operation forget` command to make sure the import operation is removed from the list of operations:

```bash
{{ ydb-cli }} -p db1 operation forget "ydb://import/8?id=281474976788395&kind=s3"
```

### List of import operations {#list}

To get a list of import operations, run the `operation list import/s3` command:

```bash
{{ ydb-cli }} -p db1 operation list import/s3
```

The format of the `operation list` command output is also specified in the `--format` option.

## Examples {#examples}

{% include [example_db1.md](../../_includes/example_db1.md) %}

### Importing data to the DB root {#example-full-db}

Importing the contents of the `export1` directory in the `mybucket` bucket to the root of the database, using S3 authentication parameters from environment variables or the `~/.aws/credentials` file:

```
ydb -p db1 import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --item src=export1,dst=.
```

### Importing multiple directories {#example-specific-dirs}

Importing objects from the dir1 and dir2 directories of the `mybucket` S3 bucket to the same-name DB directories using explicitly specified authentication parameters in S3:

```
ydb -p db1 import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key VJGSOScgs-5kDGeo2hO9 --secret-key fZ_VB1Wi5-fdKSqH6074a7w0J4X0 \
  --item src=export/dir1,dst=dir1 --item src=export/dir2,dst=dir2
```

### Getting operation IDs {#example-list-oneline}

To get a list of import operation IDs in a format that is convenient for processing in bash scripts, use [jq](https://stedolan.github.io/jq/download/):

```bash
{{ ydb-cli }} -p db1 operation list import/s3 --format proto-json-base64 | jq -r ".operations[].id"
```

You'll get an output where each new line contains the operation ID. For example:

```
ydb://import/8?id=281474976789577&kind=s3
ydb://import/8?id=281474976789526&kind=s3
ydb://import/8?id=281474976788779&kind=s3
```

These IDs can be used, for example, to run a loop that will end all current operations:

```bash
{{ ydb-cli }} -p db1 operation list import/s3 --format proto-json-base64 | jq -r ".operations[].id" | while read line; do {{ ydb-cli }} -p db1 operation forget $line;done
```

