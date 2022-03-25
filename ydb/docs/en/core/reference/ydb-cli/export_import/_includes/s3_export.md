# Exporting data to S3-compatible storage

Running the `export s3` command starts, on the server side, exporting data and information about data schema objects to S3-compatible storage in the format described in the [File structure](../file_structure.md) article:

```bash
{{ ydb-cli }} [connection options] export s3 [options]
```

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

## Command line parameters {#pars}

`[options]`: Command parameters:

### S3 connection parameters {#s3-conn}

To run the command to export data to S3, make sure to specify the [S3 connection parameters](../s3_conn.md). Since data export is performed asynchronously by the YDB server, the specified endpoint must be available to establish a server-side connection.

### List of exported objects {#items}

`--item STRING`: Description of the object to export. The `--item` parameter can be specified several times if you need to export multiple objects. The `STRING` format is `<property>=<value>,...`, with the following properties required:

- `source`, `src`, or `s`: Path to the directory or table to be exported, where `.` indicates the DB root directory. In the specified directory, the following are exported: any objects whose names do not begin with a dot and, recursively, any subdirectories whose names do not begin with a dot.
- `destination`, `dst`, or `d`: Path to S3 (key prefix) to store the exported objects to.

`--exclude STRING`: Pattern ([PCRE](https://www.pcre.org/original/doc/html/pcrepattern.html)) for excluding paths from the export destination. This parameter can be specified several times for different patterns.

### Additional parameters {#aux}

`--description STRING`: Operation text description stored in the history of operations. `--retries NUM`: Number of import retries the server will make. Defaults to 10.
`--format STRING`: Result output format.

- `pretty`: Human-readable format (default).
- `proto-json-base64`: Protobuf that supports JSON values encoded as binary strings using base64 encoding.

## Exporting data {#exec}

### Export result {#result}

If successful , the `export s3` command outputs summary information about the enqueued operation for exporting data to S3 in the format specified in the `--format` option. The actual export operation is performed by the server asynchronously. The summary displays the operation ID that can be used later to check the status and actions with the operation:

- In the `pretty` output mode used by default, the operation identifier is output in the id field with semigraphics formatting:

  ```
  ┌───────────────────────────────────────────┬───────┬─────...
  | id                                        | ready | stat...
  ├───────────────────────────────────────────┼───────┼─────...
  | ydb://export/6?id=281474976788395&kind=s3 | true  | SUCC...
  ├╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴...
  | StorageClass: NOT_SET                                      
  | Items:
  ...                                                   
  ```

- In proto-json-base64 output mode, the ID is in the "id" attribute:

  ```
  {"id":"ydb://export/6?id=281474976788395&kind=s3","ready":true, ... }
  ```

### Export status {#status}

Data is exported in the background. You can get information about the status and progress of the export operation by running the `operation get` command with the **quoted** operation ID passed as the command parameter. For example:

```bash
{{ ydb-cli }} -p db1 operation get "ydb://export/6?id=281474976788395&kind=s3"
```

The format of the `operation get` command output is also specified in the `--format` option.

Although the operation ID format is URL, there is no guarantee that it's retained later. It should only be interpreted as a string.

You can track the completion of the export operation by changes in the "progress" attribute:

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

### Ending the export operation {#forget}

When exporting data, a directory named `export_*` is created in the DB root directory, where `*` is the numeric part of the export ID. This directory stores tables containing a consistent snapshot of the exported data as of the start of the export operation.

Once the data is exported, use the `operation forget` command to make sure the export operation is ended, that is, removed from the list of operations along with deleting all the files created for it:

```bash
{{ ydb-cli }} -p db1 operation forget "ydb://export/6?id=281474976788395&kind=s3"
```

### List of export operations {#list}

To get a list of export operations, run the `operation list export/s3` command:

```bash
{{ ydb-cli }} -p db1 operation list export/s3
```

The format of the `operation list` command output is also specified in the `--format` option.

## Examples {#examples}

{% include [example_db1.md](../../_includes/example_db1.md) %}

### Exporting a database {#example-full-db}

Exporting all DB objects whose names do not begin with a dot and are not placed inside directories whose names begin with a dot to the `export1` directory in the `mybucket` bucket, using S3 authentication parameters from environment variables or the `~/.aws/credentials` file:

```
ydb -p db1 export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --item src=.,dst=export1
```

### Exporting multiple directories {#example-specific-dirs}

Exporting objects from DB directories named dir1 and dir2 to the `export1` directory in the `mybucket` bucket using explicitly specified S3 authentication parameters:

```
ydb -p db1 export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key VJGSOScgs-5kDGeo2hO9 --secret-key fZ_VB1Wi5-fdKSqH6074a7w0J4X0 \
  --item src=dir1,dst=export1/dir1 --item src=dir2,dst=export1/dir2
```

### Getting operation IDs {#example-list-oneline}

To get a list of export operation IDs in a format that is convenient for processing in bash scripts, use [jq](https://stedolan.github.io/jq/download/):

```bash
{{ ydb-cli }} -p db1 operation list export/s3 --format proto-json-base64 | jq -r ".operations[].id"
```

You'll get an output where each new line contains the operation ID. For example:

```
ydb://export/6?id=281474976789577&kind=s3
ydb://export/6?id=281474976789526&kind=s3
ydb://export/6?id=281474976788779&kind=s3
```

These IDs can be used, for example, to run a loop that will end all current operations:

```bash
{{ ydb-cli }} -p db1 operation list export/s3 --format proto-json-base64 | jq -r ".operations[].id" | while read line; do {{ ydb-cli }} -p db1 operation forget $line;done
```

