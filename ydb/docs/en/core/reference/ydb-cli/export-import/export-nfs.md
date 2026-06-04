# Exporting to NFS

The `export nfs` command starts a server-side export to the [Network File System](https://en.wikipedia.org/wiki/Network_File_System) (NFS) mounted on {{ ydb-short-name }} cluster hosts of data and schema object metadata in the format described in [File structure](./file-structure.md):

```bash
{{ ydb-cli }} [connection options] export nfs [options]
```

{% include [conn_options_ref.md](../commands/_includes/conn_options_ref.md) %}

{% note warning %}

{% include [export-supported-object-types.md](_includes/export-supported-object-types.md) %}

{% endnote %}

## Command line parameters {#pars}

`[options]`: Command parameters:

### NFS parameters {#nfs-params}

The export to NFS command requires a mounted directory (or subdirectory) shared by all objects involved in the export. Because export runs asynchronously on all {{ ydb-short-name }} hosts, the directory must exist on every host and be mounted via NFS.

`--fs-path PATH`: Path to the mounted directory (or subdirectory).

### Exported schema objects {#items}

{% include [export-root-include-exclude-params.md](_includes/export-root-include-exclude-params.md) %}

{% cut "Alternate syntax" %}

An alternate syntax is supported to specify the list of objects:

`--item STRING`: Description of the item to export. You can specify the `--item` parameter multiple times if you need to export multiple items. `STRING` is set in `<property>=<value>,...` format with the following mandatory properties:

- `source`, `src`, or `s`: Path to the exported directory or table; `.` indicates the database root directory. If you specify a directory, all non-system objects in it and, recursively, all non-system subdirectories are exported.
- `destination`, `dst`, or `d`: Path in NFS (relative to `--fs-path`) to store exported items.

`--exclude STRING`: Template ([PCRE](https://www.pcre.org/original/doc/html/pcrepattern.html)) to exclude paths from export. You may specify this parameter multiple times for different templates.

{% include [export-alternative-syntax-warning.md](_includes/export-alternative-syntax-warning.md) %}

{% endcut %}

{% include [export-aux-params-table.md](_includes/export-aux-params-table.md) %}

## Running the export {#exec}

{% include [server-export-workflow.md](_includes/server-export-workflow.md) %}

### Export result {#result}

If successful, the `export nfs` command prints summary information about the enqueued export operation to NFS in the format specified by the `--format` option. The actual export is performed by the server asynchronously. The summary shows the operation ID that you can use later to check the operation status and perform actions on it:

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

To get a list of export operations, run the `operation list export/nfs` command:

```bash
{{ ydb-cli }} -p quickstart operation list export/nfs
```

{% include [export-operation-list-tail.md](_includes/export-operation-list-tail.md) %}

## Examples {#examples}

{% include [ydb-cli-profile.md](../../../_includes/ydb-cli-profile.md) %}

### Exporting a database {#example-full-db}

Exporting all non-system database objects to the `/mnt/nfs/backups/export1` directory on the filesystem:

```bash
{{ ydb-cli }} -p quickstart export nfs \
  --fs-path /mnt/nfs/backups/export1
```

### Exporting multiple directories {#example-specific-dirs}

Exporting objects from database directories `dir1` and `dir2` to the `/mnt/nfs/backups/export1` directory on the filesystem:

```bash
{{ ydb-cli }} -p quickstart export nfs \
  --fs-path /mnt/nfs/backups/export1 \
  --include dir1 --include dir2
```

Or using the alternate syntax:

```bash
{{ ydb-cli }} -p quickstart export nfs \
  --fs-path /mnt/nfs/backups \
  --item src=dir1,dst=export1/dir1 --item src=dir2,dst=export1/dir2
```

### Exporting with encryption {#example-encryption}

Exporting the whole database with encryption:

- Using the `AES-128-GCM` encryption algorithm
- Generating a random key with `openssl` to the file `~/my_secret_key`
- Reading the generated key from the file `~/my_secret_key`
- To the `/mnt/nfs/backups/export1` directory on the filesystem

```bash
openssl rand -out ~/my_secret_key 16
{{ ydb-cli }} -p quickstart export nfs \
  --fs-path /mnt/nfs/backups/export1 \
  --encryption-algorithm AES-128-GCM --encryption-key-file ~/my_secret_key
```

Exporting database directory `dir1` with encryption:

- Using the `AES-256-GCM` encryption algorithm
- Generating a random key with `openssl` to the `YDB_ENCRYPTION_KEY` environment variable
- Reading the generated key from the `YDB_ENCRYPTION_KEY` environment variable
- To the `/mnt/nfs/backups/export1` directory on the filesystem

```bash
export YDB_ENCRYPTION_KEY=$(openssl rand -hex 32)
{{ ydb-cli }} -p quickstart export nfs \
  --root-path dir1 \
  --fs-path /mnt/nfs/backups/export1 \
  --encryption-algorithm AES-256-GCM
```

### Getting operation IDs {#example-list-oneline}

To get a list of export operation IDs in a format suitable for bash scripts, use the [jq](https://stedolan.github.io/jq/download/) utility:

```bash
{{ ydb-cli }} -p quickstart operation list export/nfs --format proto-json-base64 | jq -r ".operations[].id"
```

You'll get output where each line contains an operation ID, for example:

```text
ydb://export/6?id=281474976789577&kind=fs
ydb://export/6?id=281474976789526&kind=fs
ydb://export/6?id=281474976788779&kind=fs
```

You can use these IDs, for example, to run a loop that completes all current operations:

```bash
{{ ydb-cli }} -p quickstart operation list export/nfs --format proto-json-base64 | jq -r ".operations[].id" | while read line; do {{ ydb-cli }} -p quickstart operation forget $line;done
```
