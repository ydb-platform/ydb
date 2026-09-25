# Import from NFS

The `import nfs` command starts a server-side process of importing data and schema object information from the network file system ( [Network File System](https://en.wikipedia.org/wiki/Network_File_System), NFS) of the {{ ydb-short-name }} cluster hosts, in the format described in the [File structure](./file-structure.md) article:


```bash
{{ ydb-cli }} [connection options] import nfs [options]
```


{% include [conn_options_ref.md](../commands/_includes/conn_options_ref.md) %}

Unlike the [`tools restore` command](./tools-restore.md), the `import nfs` command always creates objects entirely, so for it to succeed, none of the imported objects (neither directories nor tables) should exist.

If you need to additionally load data into existing tables, use the [`tools restore` command](./tools-restore.md) directly on the mounted NFS directory.

## Command-line parameters {#pars}

`[options]` — command parameters:

### NFS parameters {#nfs-params}

The import from NFS command requires specifying a mounted directory (or subdirectory) common to all objects involved in the import. Since the import is performed asynchronously on all {{ ydb-short-name }} hosts, the specified directory must exist on each {{ ydb-short-name }} host and be mounted in NFS.

`--fs-path PATH`: path to the mounted directory (or subdirectory).

### Imported database schema objects {#objects}

{% include [import-objects-params.md](_includes/import-objects-params.md) %}

{% cut "Alternative method" %}

{% include [import-alternative-syntax.md](_includes/import-alternative-syntax.md) %}

- `source`, `src`, or `s` — path in NFS (relative to `fs-path`) with the imported directory or table.
- `destination`, `dst`, or `d` — the path in the database for placing the imported directory or table. The final path element must not exist. All directories along the path will be created if they do not exist.

{% include [import-alternative-syntax-warning.md](_includes/import-alternative-syntax-warning.md) %}

{% endcut %}

### Additional parameters {#aux}

{% include [import-additional-params.md](_includes/import-additional-params.md) %}

## Running the import {#exec}

{% include [server-import-workflow.md](_includes/server-import-workflow.md) %}

### Launch result {#result}

Upon successful execution, the `import nfs` command outputs summary information about the queued import from NFS operation, in the format specified by the `--format` option. The actual import is performed asynchronously by the server. The summary information includes the operation ID, which can later be used to check the status and perform actions on the operation:

{% include [import-operation-result-pretty-intro.md](_includes/import-operation-result-pretty-intro.md) %}


```text
┌───────────────────────────────────────────┬───────┬─────...
| id                                        | ready | stat...
├───────────────────────────────────────────┼───────┼─────...
| ydb://import/8?id=281474976788395&kind=fs | true  | SUCC...
├╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴...
| Items:
...
```


{% include [import-operation-result-json-intro.md](_includes/import-operation-result-json-intro.md) %}


```json
{"id":"ydb://import/8?id=281474976788395&kind=fs","ready":true, ... }
```


### Import status {#status}

{% include [import-operation-status-intro.md](_includes/import-operation-status-intro.md) %}


```bash
{{ ydb-cli }} -p quickstart operation get "ydb://import/8?id=281474976788395&kind=fs"
```


{% include [import-operation-status-after-get.md](_includes/import-operation-status-after-get.md) %}

### Completing the import operation {#forget}

{% include [import-operation-forget-intro.md](_includes/import-operation-forget-intro.md) %}


```bash
{{ ydb-cli }} -p quickstart operation forget "ydb://import/8?id=281474976788395&kind=fs"
```


### List of import operations {#list}

To get a list of import operations, use the `operation list import/nfs` command:


```bash
{{ ydb-cli }} -p quickstart operation list import/nfs
```


{% include [import-operation-list-tail.md](_includes/import-operation-list-tail.md) %}

## Examples {#examples}

{% include [ydb-cli-profile.md](../../../_includes/ydb-cli-profile.md) %}

### Importing to the database root {#example-full-db}

Importing the contents of the `/mnt/nfs/backups/export1` directory on the file system into the database root:


```bash
{{ ydb-cli }} -p quickstart import nfs \
  --fs-path /mnt/nfs/backups/export1
```


### Importing multiple directories {#example-specific-dirs}

Importing objects from the `dir1` and `dir2` directories of the export located in `/mnt/nfs/backups/export1` on the file system into the identically named database directories:


```bash
{{ ydb-cli }} -p quickstart import nfs \
  --fs-path /mnt/nfs/backups/export1 \
  --include dir1 --include dir2
```


### Importing an encrypted export {#example-encryption}

Importing a single table that was exported at path `dir/my_table` into the path `dir1/dir/my_table` from an encrypted export located in `/mnt/nfs/backups/export1` on the file system, using a secret key from the `~/my_secret_key` file.


```bash
{{ ydb-cli }} -p quickstart import nfs \
  --fs-path /mnt/nfs/backups/export1 --destination-path dir1 \
  --include dir/my_table \
  --encryption-key-file ~/my_secret_key
```


### Getting operation IDs {#example-list-oneline}

To get a list of import operation IDs in a format convenient for processing in bash scripts, you can use the [jq](https://stedolan.github.io/jq/download/) utility:


```bash
{{ ydb-cli }} -p quickstart operation list import/nfs --format proto-json-base64 | jq -r ".operations[].id"
```


You will get output where each new line contains an operation ID, for example:


```text
ydb://import/8?id=281474976789577&kind=fs
ydb://import/8?id=281474976789526&kind=fs
ydb://import/8?id=281474976788779&kind=fs
```


These IDs can be used, for example, to run a loop to complete all current operations:


```bash
{{ ydb-cli }} -p quickstart operation list import/nfs --format proto-json-base64 | jq -r ".operations[].id" | while read line; do {{ ydb-cli }} -p quickstart operation forget $line;done
```
