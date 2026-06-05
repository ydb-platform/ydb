# Exporting data to S3-compatible storage

The `export s3` command starts a server-side export to S3-compatible storage of data and schema object metadata in the format described in [File structure](../file-structure.md):

```bash
{{ ydb-cli }} [connection options] export s3 [options]
```

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

{% note warning %}

{% include [export-supported-object-types.md](export-supported-object-types.md) %}

For a simpler export of individual row-oriented and column-oriented tables to S3-compatible storage, you can use [external data sources](../../../../concepts/datamodel/external_data_source.md). For details, see [{#T}](../../../../concepts/query_execution/federated_query/s3/write_data.md#export-to-s3).

{% endnote %}

## Command line parameters {#pars}

`[options]`: Command parameters:

### S3 parameters {#s3-params}

The export to S3 command requires [S3 connection parameters](../auth-s3.md). Because export is performed asynchronously by the {{ ydb-short-name }} server, the specified endpoint must be reachable for connections from the server side.

`--destination-prefix PREFIX`: Key prefix in the S3 bucket.

### Exported schema objects {#items}

{% include [export-root-include-exclude-params.md](export-root-include-exclude-params.md) %}

{% cut "Alternate syntax" %}

An alternate syntax is supported to specify the list of objects:

`--item STRING`: Description of the item to export. You can specify the `--item` parameter multiple times if you need to export multiple items. `STRING` is set in `<property>=<value>,...` format with the following mandatory properties:

- `source`, `src`, or `s`: Path to the exported directory or table; `.` indicates the database root directory. If you specify a directory, all non-system objects in it and, recursively, all non-system subdirectories are exported.
- `destination`, `dst`, or `d`: Path (key prefix) in S3 storage to store exported items.

`--exclude STRING`: Template ([PCRE](https://www.pcre.org/original/doc/html/pcrepattern.html)) to exclude paths from export. You may specify this parameter multiple times for different templates.

{% include [export-alternative-syntax-warning.md](export-alternative-syntax-warning.md) %}

{% endcut %}

### Additional parameters {#aux}

| Parameter | Description |
| --- | --- |
| `--description STRING` | Operation text description saved to the history of operations. |
| `--retries NUM` | Number of export retries to be made by the server.<br/>Defaults to `10`. |
| `--compression STRING` | Compress exported data.<br/>If the default compression level is used for the [Zstandard](https://en.wikipedia.org/wiki/Zstandard) algorithm, data can be compressed by 5–10 times. Compressing data uses CPU resources and may affect the speed of performing other DB operations.<br/>Possible values:<br/><ul><li>`zstd`: Compression using the Zstandard algorithm with the default compression level (`3`).</li><li>`zstd-N`: Compression using the Zstandard algorithm, where `N` is the compression level (`1` — `22`).</li></ul> |
| `--encryption-algorithm ALGORITHM` | Encrypt exported data using the specified algorithm. Supported values: `AES-128-GCM`, `AES-256-GCM`, `ChaCha20-Poly1305`. |
| `--encryption-key-file PATH` | File path containing the encryption key (only for encrypted exports). The file is binary and must contain exactly the number of bytes matching the key length for the chosen encryption algorithm (16 bytes for `AES-128-GCM`, 32 bytes for `AES-256-GCM` and `ChaCha20-Poly1305`). The key can also be provided using the `YDB_ENCRYPTION_KEY` environment variable, in hexadecimal string representation. |
| `--format STRING` | Result format.<br/>Possible values:<br/><ul><li>`pretty`: Human-readable format (default).</li><li>`proto-json-base64`: [Protocol Buffers](https://en.wikipedia.org/wiki/Protocol_Buffers) in [JSON](https://en.wikipedia.org/wiki/JSON) format; binary strings are [Base64](https://en.wikipedia.org/wiki/Base64)-encoded.</li></ul> |

## Running the export {#exec}

{% include [server-export-workflow.md](server-export-workflow.md) %}

### Export result {#result}

If successful, the `export s3` command prints summary information about the enqueued export operation to S3 in the format specified by the `--format` option. The actual export is performed by the server asynchronously. The summary shows the operation ID that you can use later to check the operation status and perform actions on it:

{% include [export-operation-result-pretty-intro.md](export-operation-result-pretty-intro.md) %}

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

{% include [export-operation-result-json-intro.md](export-operation-result-json-intro.md) %}

  ```json
  {"id":"ydb://export/6?id=281474976788395&kind=s3","ready":true, ... }
  ```

### Export status {#status}

{% include [export-operation-status-intro.md](export-operation-status-intro.md) %}

```bash
{{ ydb-cli }} -p quickstart operation get "ydb://export/6?id=281474976788395&kind=s3"
```

{% include [export-operation-status-after-get.md](export-operation-status-after-get.md) %}

### Completing the export operation {#forget}

{% include [export-operation-forget-intro.md](export-operation-forget-intro.md) %}

```bash
{{ ydb-cli }} -p quickstart operation forget "ydb://export/6?id=281474976788395&kind=s3"
```

### List of export operations {#list}

To get a list of export operations, run the `operation list export/s3` command:

```bash
{{ ydb-cli }} -p quickstart operation list export/s3
```

{% include [export-operation-list-tail.md](export-operation-list-tail.md) %}

## Examples {#examples}

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

### Exporting a database {#example-full-db}

Exporting all non-system database objects to the `export1` directory in the `mybucket` bucket using S3 authentication parameters from environment variables or the `~/.aws/credentials` file:

```bash
{{ ydb-cli }} -p quickstart export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --destination-prefix export1
```

### Exporting multiple directories {#example-specific-dirs}

Exporting objects from database directories `dir1` and `dir2` to the `export1` directory in the `mybucket` bucket using explicitly specified S3 authentication parameters:

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
- Generating a random key with `openssl` to the file `~/my_secret_key`
- Reading the generated key from the file `~/my_secret_key`
- To the `export1` path prefix in the `mybucket` S3 bucket
- Using S3 authentication parameters from environment variables or the `~/.aws/credentials` file

```bash
openssl rand -out ~/my_secret_key 16
{{ ydb-cli }} -p quickstart export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket --destination-prefix export1 \
  --encryption-algorithm AES-128-GCM --encryption-key-file ~/my_secret_key
```

Exporting database directory `dir1` with encryption:

- Using the `AES-256-GCM` encryption algorithm
- Generating a random key with `openssl` to the `YDB_ENCRYPTION_KEY` environment variable
- Reading the generated key from the `YDB_ENCRYPTION_KEY` environment variable
- To the `export1` path prefix in the `mybucket` S3 bucket
- Using S3 authentication parameters from environment variables or the `~/.aws/credentials` file

```bash
export YDB_ENCRYPTION_KEY=$(openssl rand -hex 32)
{{ ydb-cli }} -p quickstart export s3 \
  --root-path dir1 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket --destination-prefix export1 \
  --encryption-algorithm AES-256-GCM
```

### Getting operation IDs {#example-list-oneline}

To get a list of export operation IDs in a format suitable for bash scripts, use the [jq](https://stedolan.github.io/jq/download/) utility:

```bash
{{ ydb-cli }} -p quickstart operation list export/s3 --format proto-json-base64 | jq -r ".operations[].id"
```

You'll get output where each line contains an operation ID, for example:

```text
ydb://export/6?id=281474976789577&kind=s3
ydb://export/6?id=281474976789526&kind=s3
ydb://export/6?id=281474976788779&kind=s3
```

You can use these IDs, for example, to run a loop that completes all current operations:

```bash
{{ ydb-cli }} -p quickstart operation list export/s3 --format proto-json-base64 | jq -r ".operations[].id" | while read line; do {{ ydb-cli }} -p quickstart operation forget $line;done
```
