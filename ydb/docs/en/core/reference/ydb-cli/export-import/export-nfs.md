# Export to NFS

The `export nfs` command starts a server-side process of exporting data and schema object information to the network file system ( [Network File System](https://en.wikipedia.org/wiki/Network_File_System), NFS) of the {{ ydb-short-name }} cluster hosts, in the format described in the [File structure](./file-structure.md) article:


```bash
{{ ydb-cli }} [connection options] export nfs [options]
```


{% include [conn_options_ref.md](../commands/_includes/conn_options_ref.md) %}

{% note warning %}

{% include [export-supported-object-types.md](_includes/export-supported-object-types.md) %}

{% endnote %}

## Command-line parameters {#pars}

`[options]` — command parameters:

### NFS parameters {#nfs-params}

The NFS export command requires specifying a mounted directory (or subdirectory) common to all objects involved in the export. Since the export is performed asynchronously on all {{ ydb-short-name }} hosts, the specified directory must exist on each {{ ydb-short-name }} host and be mounted in NFS.

`--fs-path PATH`: path to the mounted directory (or subdirectory).

### List of exported objects {#items}

{% include [export-root-include-exclude-params.md](_includes/export-root-include-exclude-params.md) %}

{% cut "Alternative method" %}

An alternative way to specify the list of objects is supported:

`--item STRING`: Description of the export object. The `--item` parameter can be specified multiple times if you need to export several objects. `STRING` is specified in the `<property>=<value>,...` format, with the following required properties:

- `source`, `src`, or `s` — path to the exported directory or table; `.` points to the root directory of the database. When specifying a directory, all non-system objects in it are exported, as well as all non-system subdirectories recursively.
- `destination`, `dst`, or `d` — path in NFS (relative to `--fs-path`).

`--exclude STRING`: Pattern ( [PCRE](https://www.pcre.org/original/doc/html/pcrepattern.html)) to exclude paths from the export. This parameter can be specified multiple times for different patterns.

{% include [export-alternative-syntax-warning.md](_includes/export-alternative-syntax-warning.md) %}

{% endcut %}

### Additional parameters {#aux}

{% include [export-additional-params.md](_includes/export-additional-params.md) %}

## Running the export {#exec}

{% include [server-export-workflow.md](_includes/server-export-workflow.md) %}

### Launch result {#result}

On successful execution, the `export nfs` command outputs summary information about the queued NFS export operation, in the format specified by the `--format` option. The actual export is performed asynchronously by the server. The summary information includes the operation ID, which can be used later to check the status and perform actions on the operation:

{% include [export-operation-result-pretty-intro.md](_includes/export-operation-result-pretty-intro.md) %}


```text
┌───────────────────────────────────────────┬───────┬─────...
| id                                        | ready | stat...
├───────────────────────────────────────────┼───────┼─────...
| ydb://export/6?id=281474976788395&kind=fs | true  | SUCC...
├╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴...
| Include index data: false
| Items:
...
```


{% include [export-operation-result-json-intro.md](_includes/export-operation-result-json-intro.md) %}


```json
{"id":"ydb://export/6?id=281474976788395&kind=fs","ready":true, ... }
```


### Export status {#status}

{% include [export-operation-status-intro.md](_includes/export-operation-status-intro.md) %}


```bash
{{ ydb-cli }} -p quickstart operation get "ydb://export/6?id=281474976788395&kind=fs"
```


{% include [export-operation-status-after-get.md](_includes/export-operation-status-after-get.md) %}

### Completing the export operation {#forget}

{% include [export-operation-forget-intro.md](_includes/export-operation-forget-intro.md) %}


```bash
{{ ydb-cli }} -p quickstart operation forget "ydb://export/6?id=281474976788395&kind=fs"
```


### List of export operations {#list}

To get the list of export operations, use the `operation list export/nfs` command:


```bash
{{ ydb-cli }} -p quickstart operation list export/nfs
```


{% include [export-operation-list-tail.md](_includes/export-operation-list-tail.md) %}

## Examples {#examples}

{% include [ydb-cli-profile.md](../../../_includes/ydb-cli-profile.md) %}

### Exporting a database {#example-full-db}

Export all non-system database objects to the `/mnt/nfs/backups/export1` directory on the file system:


```bash
{{ ydb-cli }} -p quickstart export nfs \
  --fs-path /mnt/nfs/backups/export1
```


### Exporting multiple directories {#example-specific-dirs}

Export objects from the `dir1` and `dir2` directories of the database to the `/mnt/nfs/backups/export1` directory on the file system:


```bash
{{ ydb-cli }} -p quickstart export nfs \
  --fs-path /mnt/nfs/backups/export1 \
  --include dir1 --include dir2
```


Or using an alternative method:


```bash
{{ ydb-cli }} -p quickstart export nfs \
  --fs-path /mnt/nfs/backups \
  --item src=dir1,dst=export1/dir1 --item src=dir2,dst=export1/dir2
```


### Export with encryption {#example-encryption}

Export the entire database with encryption:

- Using the `AES-128-GCM` encryption algorithm
- Generating a random key with the `openssl` utility to the `~/my_secret_key` file
- Reading the generated key from the `~/my_secret_key` file
- To the `/mnt/nfs/backups/export1` directory on the file system


```bash
openssl rand -out ~/my_secret_key 16
{{ ydb-cli }} -p quickstart export nfs \
  --fs-path /mnt/nfs/backups/export1 \
  --encryption-algorithm AES-128-GCM --encryption-key-file ~/my_secret_key
```


Export the `dir1` directory of the database with encryption:

- Using the `AES-256-GCM` encryption algorithm
- Generating a random key with the `openssl` utility to the `YDB_ENCRYPTION_KEY` environment variable
- Reading the generated key from the `YDB_ENCRYPTION_KEY` environment variable
- To the `/mnt/nfs/backups/export1` directory on the file system


```bash
export YDB_ENCRYPTION_KEY=$(openssl rand -hex 32)
{{ ydb-cli }} -p quickstart export nfs \
  --root-path dir1 \
  --fs-path /mnt/nfs/backups/export1 \
  --encryption-algorithm AES-256-GCM
```


### Getting operation IDs {#example-list-oneline}

To get a list of export operation IDs in a format convenient for processing in bash scripts, you can use the [jq](https://stedolan.github.io/jq/download/) utility:


```bash
{{ ydb-cli }} -p quickstart operation list export/nfs --format proto-json-base64 | jq -r ".operations[].id"
```


You will get output where each new line contains an operation ID, for example:


```text
ydb://export/6?id=281474976789577&kind=fs
ydb://export/6?id=281474976789526&kind=fs
ydb://export/6?id=281474976788779&kind=fs
```


These IDs can be used, for example, to run a loop to complete all current operations:


```bash
{{ ydb-cli }} -p quickstart operation list export/nfs --format proto-json-base64 | jq -r ".operations[].id" | while read line; do {{ ydb-cli }} -p quickstart operation forget $line;done
```
