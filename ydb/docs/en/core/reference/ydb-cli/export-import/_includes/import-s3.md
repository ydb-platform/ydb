# Importing from S3-compatible storage

The `import s3` command starts the process of importing data and schema object information from an S3-compatible storage on the server side, in the format described in the article [File structure](../file-structure.md):


```bash
{{ ydb-cli }} [connection options] import s3 [options]
```


{% note info %}

Importing tables from an S3-compatible storage in other formats is possible using [external tables](../../../../concepts/query_execution/federated_query/s3/external_table.md); for more details, see the article [{#T}](../../../../concepts/query_execution/federated_query/import_and_export.md#import).

{% endnote %}

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

Unlike the [`tools restore` command](../tools-restore.md), the `import s3` command always creates objects entirely, so for it to succeed, none of the imported objects (neither directories nor tables) should exist.

If you need to load additional data into existing tables from S3, you can copy the S3 contents to the file system (for example, using [S3cmd](https://s3tools.org/s3cmd)) and use the [`tools restore` command](../tools-restore.md).

## Command-line parameters {#pars}

`[options]` — command parameters:

### S3 parameters {#s3-params}

The S3 import command requires specifying [S3 connection parameters](../auth-s3.md). Since the import is performed asynchronously by the {{ ydb-short-name }} server, the specified endpoint must be accessible for establishing a connection from the server side.

`--source-prefix PREFIX`: Import prefix in the S3 bucket.

### Imported database schema objects {#objects}

{% include [import-objects-params.md](./import-objects-params.md) %}

{% cut "Alternative method" %}

{% include [import-alternative-syntax.md](./import-alternative-syntax.md) %}

- `source`, `src`, or `s` — the S3 key prefix with the imported directory or table.
- `destination`, `dst`, or `d` — the path in the database for placing the imported directory or table. The final path element must not exist. All directories along the path will be created if they do not exist.

{% include [import-alternative-syntax-warning.md](./import-alternative-syntax-warning.md) %}

{% endcut %}

### Additional parameters {#aux}

{% include [import-additional-params.md](import-additional-params.md) %}

- `--list`: List objects in an existing export.

## Running the import {#exec}

{% include [server-import-workflow.md](server-import-workflow.md) %}

### Launch result {#result}

Upon successful execution, the `import s3` command outputs summary information about the queued S3 import operation, in the format specified by the `--format` option. The actual import is performed asynchronously by the server. The summary information includes the operation ID, which can be used later to check the status and perform actions on the operation:

{% include [import-operation-result-pretty-intro.md](import-operation-result-pretty-intro.md) %}


```text
┌───────────────────────────────────────────┬───────┬─────...
| id                                        | ready | stat...
├───────────────────────────────────────────┼───────┼─────...
| ydb://import/8?id=281474976788395&kind=s3 | true  | SUCC...
├╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴...
| Items:
...
```


{% include [import-operation-result-json-intro.md](import-operation-result-json-intro.md) %}


```json
{"id":"ydb://import/8?id=281474976788395&kind=s3","ready":true, ... }
```


### Import status {#status}

{% include [import-operation-status-intro.md](import-operation-status-intro.md) %}


```bash
{{ ydb-cli }} -p quickstart operation get "ydb://import/8?id=281474976788395&kind=s3"
```


{% include [import-operation-status-after-get.md](import-operation-status-after-get.md) %}

### Completing the import operation {#forget}

{% include [import-operation-forget-intro.md](import-operation-forget-intro.md) %}


```bash
{{ ydb-cli }} -p quickstart operation forget "ydb://import/8?id=281474976788395&kind=s3"
```


### List of import operations {#list}

To get a list of import operations, use the `operation list import/s3` command:


```bash
{{ ydb-cli }} -p quickstart operation list import/s3
```


{% include [import-operation-list-tail.md](import-operation-list-tail.md) %}

## Examples {#examples}

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

### Importing to the database root {#example-full-db}

Importing the contents of the `export1` directory in the `mybucket` bucket to the database root, using S3 authentication parameters from environment variables or the `~/.aws/credentials` file:


```bash
{{ ydb-cli }} -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --source-prefix export1
```


### Importing multiple directories {#example-specific-dirs}

Importing objects from the `dir1` and `dir2` directories of the export located in the `export1` directory in the `mybucket` bucket, into the identically named directories of the database, using explicitly specified S3 authentication parameters:


```bash
{{ ydb-cli }} -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --source-prefix export1
  --include dir1 --include dir2
```


### Listing objects in an existing encrypted export {#example-list}

Listing the paths of all objects in an existing encrypted export located in the `export1` directory in the `mybucket` bucket, using the secret key from the `~/my_secret_key` file.


```bash
{{ ydb-cli }} -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --source-prefix export1
  --encryption-key-file ~/my_secret_key
  --list
```


### Importing an encrypted export {#example-encryption}

Importing a single table that was exported along the `dir/my_table` path, to the `dir1/dir/my_table` path, from an encrypted export located under the `export1` prefix in the `mybucket` bucket, using the secret key from the `~/my_secret_key` file.


```bash
{{ ydb-cli }} -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --source-prefix export1 --destination-path dir1 \
  --include dir/my_table \
  --encryption-key-file ~/my_secret_key
```


### Getting operation IDs {#example-list-oneline}

To get a list of import operation IDs in a format convenient for processing in bash scripts, you can use the [jq](https://stedolan.github.io/jq/download/) utility:


```bash
{{ ydb-cli }} -p quickstart operation list import/s3 --format proto-json-base64 | jq -r ".operations[].id"
```


You will get output where each new line contains an operation ID, for example:


```text
ydb://import/8?id=281474976789577&kind=s3
ydb://import/8?id=281474976789526&kind=s3
ydb://import/8?id=281474976788779&kind=s3
```


These IDs can be used, for example, to run a loop to complete all current operations:


```bash
{{ ydb-cli }} -p quickstart operation list import/s3 --format proto-json-base64 | jq -r ".operations[].id" | while read line; do {{ ydb-cli }} -p quickstart operation forget $line;done
```
