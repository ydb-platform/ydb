# Export to S3-compatible storage

The `export s3` command starts a server-side process of exporting data and schema object information to an S3-compatible storage in the format described in the [File structure](../file-structure.md) article:


```bash
{{ ydb-cli }} [connection options] export s3 [options]
```


{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

{% note warning %}

{% include [export-supported-object-types.md](export-supported-object-types.md) %}

For a simpler export of single row and column tables to an S3-compatible data storage, you can use [external data sources](../../../../concepts/datamodel/external_data_source.md). For more details, see the [{#T}](../../../../concepts/query_execution/federated_query/s3/write_data.md#export-to-s3) article.

{% endnote %}

## Command-line parameters {#pars}

`[options]` — command parameters:

### S3 parameters {#s3-params}

The export to S3 command requires specifying [S3 connection parameters](../auth-s3.md). Since the export is performed asynchronously by the {{ ydb-short-name }} server, the specified endpoint must be accessible for establishing a connection from the server side.

`--destination-prefix PREFIX`: Key prefix in the S3 bucket.

### List of exported objects {#items}

{% include [export-root-include-exclude-params.md](export-root-include-exclude-params.md) %}

{% cut "Alternative method" %}

An alternative way to specify the list of objects is supported:

`--item STRING`: Description of the export object. The `--item` parameter can be specified multiple times if you need to export several objects. `STRING` is specified in the `<property>=<value>,...` format, with the following required properties:

- `source`, `src`, or `s` — path to the exported directory or table; `.` points to the root directory of the database. When specifying a directory, all non-system objects in it are exported, as well as all non-system subdirectories recursively.
- `destination`, `dst`, or `d` — path (key prefix) in S3 for placing the exported objects.

`--exclude STRING`: Pattern ( [PCRE](https://www.pcre.org/original/doc/html/pcrepattern.html)) to exclude paths from the export. This parameter can be specified multiple times for different patterns.

{% include [export-alternative-syntax-warning.md](export-alternative-syntax-warning.md) %}

{% endcut %}

### Additional parameters {#aux}

{% include [export-additional-params.md](export-additional-params.md) %}

## Running the export {#exec}

{% include [server-export-workflow.md](server-export-workflow.md) %}

### Launch result {#result}

Upon successful execution, the `export s3` command outputs summary information about the queued export operation to S3, in the format specified by the `--format` option. The actual export is performed asynchronously by the server. The summary information includes the operation ID, which can be used later to check the status and perform actions on the operation:

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

To get the list of export operations, use the `operation list export/s3` command:


```bash
{{ ydb-cli }} -p quickstart operation list export/s3
```


{% include [export-operation-list-tail.md](export-operation-list-tail.md) %}

## Examples {#examples}

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

### Exporting a database {#example-full-db}

Export all non-system database objects to the `export1` directory in the `mybucket` bucket using S3 authentication parameters from environment variables or the `~/.aws/credentials` file:


```bash
{{ ydb-cli }} -p quickstart export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --destination-prefix export1
```


### Exporting multiple directories {#example-specific-dirs}

Export objects from the `dir1` and `dir2` directories of the database to the `export1` directory in the `mybucket` bucket, using explicitly specified S3 authentication parameters:


```bash
{{ ydb-cli }} -p quickstart export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --destination-prefix export1 --include dir1 --include dir2
```


Or using an alternative method:


```bash
{{ ydb-cli }} -p quickstart export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --item src=dir1,dst=export1/dir1 --item src=dir2,dst=export1/dir2
```


### Export with encryption {#example-encryption}

Export the entire database with encryption:

- Using the `AES-128-GCM` encryption algorithm
- Generating a random key with the `openssl` utility to the `~/my_secret_key` file
- Reading the generated key from the `~/my_secret_key` file
- To the path prefix `export1` in the S3 bucket `mybucket`
- Using S3 authentication parameters from environment variables or the `~/.aws/credentials` file


```bash
openssl rand -out ~/my_secret_key 16
{{ ydb-cli }} -p quickstart export s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket --destination-prefix export1 \
  --encryption-algorithm AES-128-GCM --encryption-key-file ~/my_secret_key
```


Export the `dir1` directory of the database with encryption:

- Using the `AES-256-GCM` encryption algorithm
- Generating a random key with the `openssl` utility to the `YDB_ENCRYPTION_KEY` environment variable
- Reading the generated key from the `YDB_ENCRYPTION_KEY` environment variable
- To the path prefix `export1` in the S3 bucket `mybucket`
- Using S3 authentication parameters from environment variables or the `~/.aws/credentials` file


```bash
export YDB_ENCRYPTION_KEY=$(openssl rand -hex 32)
{{ ydb-cli }} -p quickstart export s3 \
  --root-path dir1 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket --destination-prefix export1 \
  --encryption-algorithm AES-256-GCM
```


### Getting operation IDs {#example-list-oneline}

To get a list of export operation IDs in a format convenient for processing in bash scripts, you can use the [jq](https://stedolan.github.io/jq/download/) utility:


```bash
{{ ydb-cli }} -p quickstart operation list export/s3 --format proto-json-base64 | jq -r ".operations[].id"
```


You will get output where each new line contains an operation ID, for example:


```text
ydb://export/6?id=281474976789577&kind=s3
ydb://export/6?id=281474976789526&kind=s3
ydb://export/6?id=281474976788779&kind=s3
```


These IDs can be used, for example, to run a loop to complete all current operations:


```bash
{{ ydb-cli }} -p quickstart operation list export/s3 --format proto-json-base64 | jq -r ".operations[].id" | while read line; do {{ ydb-cli }} -p quickstart operation forget $line;done
```
