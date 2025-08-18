# Exporting data to S3-compatible storage

The `export s3` command starts exporting data and schema objects details to S3-compatible storage, in the format described under [File structure](../file-structure.md):

```bash
{{ ydb-cli }} [connection options] export s3 [options]
```

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

{% note warning %}

The export feature is available only for objects of the following types:

- [Directory](../../../../concepts/datamodel/dir.md)
- [Row-oriented table](../../../../concepts/datamodel/table.md#row-oriented-tables)
- [Secondary index](../../../../concepts/glossary.md#secondary-index)
- [Vector index](../../../../concepts/glossary.md#vector-index)

{% endnote %}

## Command line parameters {#pars}

`[options]`: Command parameters:

### S3 parameters {#s3-params}

To run the command to export data to S3 storage, specify the [S3 connection parameters](../auth-s3.md). Since data is exported by the YDB server asynchronously, the specified endpoint must be available to establish a connection on the server side.

`--destination-prefix PREFIX`: Destination prefix in the S3 bucket.

### Exported schema objects {#objects}

`--root-path PATH`: Root directory for the objects being exported; defaults to the database root if not provided.

`--include PATH`: Schema objects to be included in the export. Directories are traversed recursively. Paths are relative to the `root-path`. You may specify this parameter multiple times to include several objects. If not specified, all non-system objects in the `root-path` are exported.

`--exclude STRING`: Template ([PCRE](https://www.pcre.org/original/doc/html/pcrepattern.html)) to exclude paths from export. Paths are relative to the `root-path`. You may specify this parameter multiple times for different templates.

{% cut "Alternate syntax" %}

There's an alternate syntax to specify the list of exported objects.

`--item STRING`: Description of the item to export. You can specify the `--item` parameter multiple times if you need to export multiple items. `STRING` is set in `<property>=<value>,...` format with the following mandatory properties:

- `source`, `src`, or `s`: Path to the exported directory or table, `.` indicates the DB root directory. If you specify a directory, all its child non-system objects and, recursively, all non-system subdirectories are exported.
- `destination`, `dst`, or `d`: Path (key prefix) in S3 storage to store exported items.

`--exclude STRING`: Template ([PCRE](https://www.pcre.org/original/doc/html/pcrepattern.html)) to exclude paths from export. You may specify this parameter multiple times for different templates.

{% note warning %}

The exports made using the alternate syntax will not contain a list of objects as part of the backup, so some features may not be available for them (like encryption), and imports are possible only using the corresponding alternate syntax of import.

{% endnote %}

{% endcut %}

### Additional parameters {#aux}

| Parameter | Description |
--- | ---
| `--description STRING` | Operation text description saved to the history of operations. |
| `--retries NUM` | Number of export retries to be made by the server.<br/>Defaults to `10`. |
| `--compression STRING` | Compress exported data.<br/>If the default compression level is used for the [Zstandard](https://en.wikipedia.org/wiki/Zstandard) algorithm, data can be compressed by 5-10 times. Compressing data uses the CPU and may affect the speed of performing other DB operations.<br/>Possible values:<br/><ul><li>`zstd`: Compression using the Zstandard algorithm with the default compression level (`3`).</li><li>`zstd-N`: Compression using the Zstandard algorithm, where `N` stands for the compression level (`1` — `22`).</li></ul> |
| `--encryption-algorithm ALGORITHM` | Encrypt exported data using the specified algorithm. Supported values: `AES-128-GCM`, `AES-256-GCM`, `ChaCha20-Poly1305`. |
| `--encryption-key-file PATH` | File path containing the encryption key (only for encrypted exports). The file is binary and must contain exactly the number of bytes matching the key length for the chosen encryption algorithm (16 bytes for `AES-128-GCM`, 32 bytes for `AES-256-GCM` and `ChaCha20-Poly1305`). The key can also be provided using the `YDB_ENCRYPTION_KEY` environment variable, in hexadecimal string representation. |
| `--format STRING` | Result format.<br/>Possible values:<br/><ul><li>`pretty`: Human-readable format (default).</li><li>`proto-json-base64`: [Protocol Buffers](https://en.wikipedia.org/wiki/Protocol_Buffers) in [JSON](https://en.wikipedia.org/wiki/JSON) format, binary strings are [Base64](https://en.wikipedia.org/wiki/Base64)-encoded.</li></ul> |

## Running the export command {#exec}

### Export result {#result}

If successful, the `export s3` command prints summary information about the enqueued operation to export data to S3, in the format specified in the `--format` option. The export itself is performed by the server asynchronously. The summary shows the operation ID that you can use later to check the operation status and perform actions on it:

- In the default `pretty` mode, the operation ID is displayed in the id field with semigraphics formatting:

   ```text
   ┌───────────────────────────────────────────┬───────┬─────...
   | id                                        | ready | stat...
   ├───────────────────────────────────────────┼───────┼─────...
   | ydb://export/6?id=281474976788395&kind=s3 | true  | SUCC...
   ├╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴...
   | StorageClass: NOT_SET
   | Items:
   ...
   ```

- In the `proto-json-base64` mode, the operation ID is in the "id" attribute:

   ```json
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

   ```text
   ┌───── ... ──┬───────┬─────────┬──────────┬─...
   | id         | ready | status  | progress | ...
   ├──────... ──┼───────┼─────────┼──────────┼─...
   | ydb:/...   | true  | SUCCESS | Done     | ...
   ├╴╴╴╴╴ ... ╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴┴╴...
   ...
   ```

- In the `proto-json-base64` mode, the completed export operation is indicated with the `PROGRESS_DONE` value of the `progress` attribute:

   ```json
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

Exporting all DB non-system objects to the `export1` directory in `mybucket` using the S3 authentication parameters from environment variables or the `~/.aws/credentials` file:

```bash
{{ ydb-cli }} -p quickstart export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --destination-prefix export1
```

### Exporting multiple directories {#example-specific-dirs}

Exporting items from DB directories named `dir1` and `dir2` to the `export1` directory in `mybucket` using the explicitly set S3 authentication parameters:

```bash
{{ ydb-cli }} -p quickstart export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --destination-prefix export1 --include dir1 --include dir2
```

Or using the alternate syntax:

```bash
{{ ydb-cli }} -p quickstart export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --item src=dir1,dst=export1/dir1 --item src=dir2,dst=export1/dir2
```

### Exporting with encryption {#example-encryption}

Exporting the whole database with encryption:
- Using the `AES-128-GCM` encryption algorithm
- Generating the random key using the `openssl` utility to the file `~/my_secret_key`
- Reading the generated key from the file `~/my_secret_key`
- To the `export1` path prefix in the `mybucket` S3 bucket
- Using the S3 authentication parameters from environment variables or the `~/.aws/credentials` file

```bash
openssl rand -out ~/my_secret_key 16
{{ ydb-cli }} -p quickstart export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket --destination-prefix export1 \
  --encryption-algorithm AES-128-GCM --encryption-key-file ~/my_secret_key
```

Exporting the subdirectory `dir1` of a database with encryption:
- Using the `AES-256-GCM` encryption algorithm
- Generating the random key using the `openssl` utility to the environment variable `YDB_ENCRYPTION_KEY`
- Reading the key from the environment variable `YDB_ENCRYPTION_KEY`
- To the `export1` path prefix in the `mybucket` S3 bucket
- Using the S3 authentication parameters from environment variables or the `~/.aws/credentials` file

```bash
export YDB_ENCRYPTION_KEY=$(openssl rand -hex 32)
{{ ydb-cli }} -p quickstart export s3 \
  --root-path dir1 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket --destination-prefix export1 \
  --encryption-algorithm AES-256-GCM
```

### Getting operation IDs {#example-list-oneline}

To get a list of export operation IDs in a format suitable for handling in bash scripts, use the [jq](https://stedolan.github.io/jq/download/) utility:

```bash
{{ ydb-cli }} -p quickstart operation list export/s3 --format proto-json-base64 | jq -r ".operations[].id"
```

You'll get a result where each new line shows an operation's ID. For example:

```text
ydb://export/6?id=281474976789577&kind=s3
ydb://export/6?id=281474976789526&kind=s3
ydb://export/6?id=281474976788779&kind=s3
```

You can use these IDs, for example, to run a loop to end all the current operations:

```bash
{{ ydb-cli }} -p quickstart operation list export/s3 --format proto-json-base64 | jq -r ".operations[].id" | while read line; do {{ ydb-cli }} -p quickstart operation forget $line;done
```
