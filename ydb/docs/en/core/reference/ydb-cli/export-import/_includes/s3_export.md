# Exporting data to S3-compatible storage

The `export s3` command starts exporting data and information on the server side about data schema objects to S3-compatible storage, in the format described under [File structure](../file-structure.md):

```bash
{{ ydb-cli }} [connection options] export s3 [options]
```

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

## Command line parameters {#pars}

`[options]`: Command parameters:

### S3 connection parameters {#s3-conn}

To run the command to export data to S3 storage, specify the [S3 connection parameters](../auth-s3.md). Since data is exported by the YDB server asynchronously, the specified endpoint must be available to establish a connection on the server side.

### List of exported items {#items}

`--item STRING`: Description of the item to export. You can specify the `--item` parameter multiple times if you need to export multiple items. `STRING` is set in `<property>=<value>,...` format with the following mandatory properties:
- `source`, `src`, or `s`: Path to the exported directory or table, `.` indicates the DB root directory. If you specify a directory, all of its items whose names do not start with a dot and, recursively, all subdirectories whose names do not start with a dot are exported.
- `destination`, `dst`, or `d`: Path (key prefix) in S3 storage to store exported items.

`--exclude STRING`: Template ([PCRE](https://www.pcre.org/original/doc/html/pcrepattern.html)) to exclude paths from export. Specify this parameter multiple times for different templates.

### Additional parameters {#aux}

| Parameter | Description |
--- | ---
| `--description STRING` | Operation text description saved to the history of operations. |
| `--retries NUM` | Number of export retries to be made by the server.<br/>Defaults to `10`. |
| `--compression STRING` | Compress exported data.<br/>If the default compression level is used for the [Zstandard](https://en.wikipedia.org/wiki/Zstandard) algorithm, data can be compressed by 5-10 times. Compressing data uses the CPU and may affect the speed of performing other DB operations.<br/>Possible values:<br/><ul><li>`zstd`: Compression using the Zstandard algorithm with the default compression level (`3`).</li><li>`zstd-N`: Compression using the Zstandard algorithm, where `N` stands for the compression level (`1` — `22`).</li></ul> |
| `--format STRING` | Result format.<br/>Possible values:<br/><ul><li>`pretty`: Human-readable format (default).</li><li>`proto-json-base64`: [Protocol Buffers](https://en.wikipedia.org/wiki/Protocol_Buffers) in [JSON](https://en.wikipedia.org/wiki/JSON) format, binary strings are [Base64](https://en.wikipedia.org/wiki/Base64)-encoded.</li></ul> |

## Running the export command {#exec}

### Export result {#result}

If successful, the `export s3` command prints summary information about the enqueued operation to export data to S3, in the format specified in the `--format` option. The export itself is performed by the server asynchronously. The summary shows the operation ID that you can use later to check the operation status and perform actions on it:

- In the default `pretty` mode, the operation ID is displayed in the id field with semigraphics formatting:

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

- In the proto-json-base64 mode, the operation ID is in the "id" attribute:

   ```
   {"id":"ydb://export/6?id=281474976788395&kind=s3","ready":true, ... }
   ```

### Export status {#status}

Data is exported in the background. To find out the export status and progress, use the `operation get` command with the operation ID **enclosed in quotation marks** and passed as a command parameter. For example:

```bash
{{ ydb-cli }} -p quickstart operation get "ydb://export/6?id=281474976788395&kind=s3"
```

The `operation get` format is also set by the `--format` option.

Although the operation ID is in URL format, there is no guarantee that it is maintained in the future. It should only be interpreted as a string.

You can track the export progress by changes in the "progress" attribute:

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

### Completing the export operation {#forget}

When running the export operation, a directory named `export_*` is created in the root directory, where `*` is the numeric part of the export ID. This directory stores tables with a consistent snapshot of exported data as of the export start time.

Once the export is done, use the `operation forget` command to make sure the export is completed: the operation is removed from the list of operations and all files created for it are deleted:

```bash
{{ ydb-cli }} -p quickstart operation forget "ydb://export/6?id=281474976788395&kind=s3"
```

### List of export operations {#list}

To get a list of export operations, run the `operation list export/s3` command:

```bash
{{ ydb-cli }} -p quickstart operation list export/s3
```

The `operation list` format is also set by the `--format` option.

## Examples {#examples}

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

### Exporting a database {#example-full-db}

Exporting all DB objects whose names do not start with a dot and that are not stored in directories whose names start with a dot to the `export1` directory in `mybucket` using the S3 authentication parameters from environment variables or the `~/.aws/credentials` file:

```
ydb -p quickstart export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --item src=.,dst=export1
```

### Exporting multiple directories {#example-specific-dirs}

Exporting items from DB directories named dir1 and dir2 to the `export1` directory in `mybucket` using the explicitly set S3 authentication parameters:

```
ydb -p quickstart export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key VJGSOScgs-5kDGeo2hO9 --secret-key fZ_VB1Wi5-fdKSqH6074a7w0J4X0 \
  --item src=dir1,dst=export1/dir1 --item src=dir2,dst=export1/dir2
```

### Getting operation IDs {#example-list-oneline}

To get a list of export operation IDs in a format suitable for handling in bash scripts, use the [jq](https://stedolan.github.io/jq/download/) utility:

```bash
{{ ydb-cli }} -p quickstart operation list export/s3 --format proto-json-base64 | jq -r ".operations[].id"
```

You'll get a result where each new line shows an operation's ID. For example:

```
ydb://export/6?id=281474976789577&kind=s3
ydb://export/6?id=281474976789526&kind=s3
ydb://export/6?id=281474976788779&kind=s3
```

You can use these IDs, for example, to run a loop to end all the current operations:

```bash
{{ ydb-cli }} -p quickstart operation list export/s3 --format proto-json-base64 | jq -r ".operations[].id" | while read line; do {{ ydb-cli }} -p quickstart operation forget $line;done
```
