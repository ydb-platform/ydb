# Importing from NFS

The `import nfs` command starts a server-side import from the [Network File System](https://en.wikipedia.org/wiki/Network_File_System) (NFS) mounted on {{ ydb-short-name }} cluster hosts of data and schema object metadata in the format described in [File structure](./file-structure.md):

```bash
{{ ydb-cli }} [connection options] import nfs [options]
```

{% include [conn_options_ref.md](../commands/_includes/conn_options_ref.md) %}

Unlike the [`tools restore` command](./tools-restore.md), the `import nfs` command always creates objects in entirety, so none of the imported objects (directories or tables) must already exist for a successful run.

If you need to load more data into existing tables, use the [`tools restore`](./tools-restore.md) command directly on the mounted NFS directory.

## Command line parameters {#pars}

`[options]`: Command parameters:

### NFS parameters {#nfs-params}

The import from NFS command requires a mounted directory (or subdirectory) shared by all objects involved in the import. Because import runs asynchronously on all {{ ydb-short-name }} hosts, the directory must exist on every host and be mounted via NFS.

`--fs-path PATH`: Path to the mounted directory (or subdirectory).

### Imported schema objects {#objects}

{% include [import-objects-params.md](_includes/import-objects-params.md) %}

{% cut "Alternate syntax" %}

{% include [import-alternative-syntax.md](_includes/import-alternative-syntax.md) %}

- `source`, `src`, or `s`: Path in NFS (relative to `fs-path`) that contains the imported directory or table.
- `destination`, `dst`, or `d`: Database path to host the imported directory or table. The final path element must not exist. All directories along the path are created if missing.

{% include [import-alternative-syntax-warning.md](_includes/import-alternative-syntax-warning.md) %}

{% endcut %}

{% include [import-aux-params-table.md](_includes/import-aux-params-table.md) %}

| `--skip-checksum-validation` | Skip validation of imported objects' [checksums](./file-structure.md#checksums). |

## Running the import {#exec}

{% include [server-import-workflow.md](_includes/server-import-workflow.md) %}

### Import result {#result}

If successful, the `import nfs` command prints summary information about the enqueued import operation from NFS in the format specified by the `--format` option. The actual import is performed by the server asynchronously. The summary shows the operation ID that you can use later to check the operation status and perform actions on it:

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

To get a list of import operations, run the `operation list import/nfs` command:

```bash
{{ ydb-cli }} -p quickstart operation list import/nfs
```

{% include [import-operation-list-tail.md](_includes/import-operation-list-tail.md) %}

## Examples {#examples}

{% include [ydb-cli-profile.md](../../../_includes/ydb-cli-profile.md) %}

### Importing to the database root {#example-full-db}

Importing to the database root the contents of the `/mnt/nfs/backups/export1` directory on the filesystem:

```bash
{{ ydb-cli }} -p quickstart import nfs \
  --fs-path /mnt/nfs/backups/export1
```

### Importing multiple directories {#example-specific-dirs}

Importing objects from the `dir1` and `dir2` directories of an export located in `/mnt/nfs/backups/export1` on the filesystem into the same-name database directories:

```bash
{{ ydb-cli }} -p quickstart import nfs \
  --fs-path /mnt/nfs/backups/export1 \
  --include dir1 --include dir2
```

### Importing encrypted export {#example-encryption}

Importing one table that was exported to the path `dir/my_table` into the path `dir1/dir/my_table` from an encrypted export located in `/mnt/nfs/backups/export1` on the filesystem, using the secret key from the `~/my_secret_key` file:

```bash
{{ ydb-cli }} -p quickstart import nfs \
  --fs-path /mnt/nfs/backups/export1 --destination-path dir1 \
  --include dir/my_table \
  --encryption-key-file ~/my_secret_key
```

### Getting operation IDs {#example-list-oneline}

To get a list of import operation IDs in a format suitable for bash scripts, use the [jq](https://stedolan.github.io/jq/download/) utility:

```bash
{{ ydb-cli }} -p quickstart operation list import/nfs --format proto-json-base64 | jq -r ".operations[].id"
```

You'll get output where each line contains an operation ID, for example:

```text
ydb://import/8?id=281474976789577&kind=fs
ydb://import/8?id=281474976789526&kind=fs
ydb://import/8?id=281474976788779&kind=fs
```

You can use these IDs, for example, to run a loop that completes all current operations:

```bash
{{ ydb-cli }} -p quickstart operation list import/nfs --format proto-json-base64 | jq -r ".operations[].id" | while read line; do {{ ydb-cli }} -p quickstart operation forget $line;done
```
