# Validating a backup

The `tools validate` command checks integrity of a backup created by [`export s3`](./export-s3.md) or [`export nfs`](./export-nfs.md). The check runs in this process and does not restore data into a database, so a {{ ydb-short-name }} connection is not required.

```bash
{{ ydb-cli }} tools validate s3 [options]
{{ ydb-cli }} tools validate nfs [options]
```

`s3` and `nfs` differ only in how the backup is read. File layout is described in [File structure of data export](./file-structure.md).

## What is checked {#checks}

The path is a full backup when its `metadata.json` has `"kind": "SimpleExportV0"`. The command then checks `SchemaMapping` and every object listed there. This metadata is written when the export uses `--destination-prefix` or `--fs-path`.

Exports created with `--item src=...,dst=...` do not write that file or `SchemaMapping`. The destination prefix is a directory of exported objects (`scheme.pb`, `create_view.sql`, and the other schema files). The command finds those objects and checks each of them, including index implementation tables stored under a table even when the table metadata does not list indexes.

`--expected-objects FILE` supplies the missing manifest for this layout. Each line is an object name relative to the validated path. An object found in the backup and absent from the file is printed as a warning and does not fail the command. An object listed in the file and absent from the backup is an error and the command exits with code 1. An index implementation table stored under a listed object is treated as part of that object.

Any other path is one exported object (a table, a view, a topic, and so on). A path to one table checks that table, including indexes and changefeeds recorded in its `metadata.json` and index tables found under it. A path to a full backup checks the whole backup.

Metadata files (`scheme.pb`, `permissions.pb`, `metadata.json`, changefeed and topic descriptions, schema SQL and proto files, `SchemaMapping`) must be readable and contain the minimum fields required to describe the object. For a table this includes a non-empty column list, a type for every column, and a primary key that is a subset of those columns.

The partition count in `scheme.pb` must match the set of `data_...` files. `uniform_partitions` is that count. `partition_at_keys` means `split_points` plus one. If neither is set, the table has one partition, `data_00`. Indexes must be `0 .. N-1` with no gaps, duplicates, or extra files. Canonical names are `data_00.csv`, `data_01.parquet`, and so on (at least two digits), optionally with `.zst`.

Without `--scheme-only`, data file contents are checked too. Export checksums are SHA-256 of uncompressed plaintext, stored in a sidecar such as `data_00.csv.sha256` (the same name is used when the object is `data_00.csv.zst`). Metadata and schema sidecars are checked in both modes. In `--scheme-only` mode data bytes are not read; the command only checks that each data sidecar exists and contains a SHA-256 hex digest.

Content validation requires those sidecars. Exports created with checksums (the default since 25.3) write `"checksum": "sha256"` in the backup `metadata.json` and `"version": 1` in object metadata. If the sidecars are absent, validation without `--scheme-only` fails.

Encrypted backups (`.enc` objects or an `encryption` field in the backup metadata) are detected and rejected. The command accepts the same encryption key options as `import`, but it does not decrypt backup files.

## Command line parameters {#pars}

### Common parameters {#common}

| Parameter | Description |
| --- | --- |
| `--scheme-only` | Check file composition and metadata structure only. Do not read data file contents. |
| `--retries NUM` | Attempts to read a backup file after an I/O error. Default: `10`. |
| `--encryption-key-file PATH` | Path to the encryption key file, same encoding as [`import s3`](./import-s3.md) / [`import nfs`](./import-nfs.md). The key can also be passed in `YDB_ENCRYPTION_KEY` as a hexadecimal string, or the file path in `YDB_ENCRYPTION_KEY_FILE`. Encrypted files are still rejected. |
| `--item PROPERTY=VALUE,...` | Object to validate. Can be repeated. Properties: `source` (`src`, `s`) is the backup path; `destination` (`dst`, `d`) is accepted for compatibility with `import` and ignored. |
| `--expected-objects PATH` | Text file with expected object names for a backup created with `--item`, one name per line, relative to the validated path. Empty lines are ignored. Extra objects in the backup are warnings. Names missing from the backup are errors. |

### S3 parameters {#s3}

Connection parameters match [`import s3`](./import-s3.md). See [Connecting to and authenticating with S3](./auth-s3.md).

| Parameter | Description |
| --- | --- |
| `--s3-endpoint ENDPOINT` | S3 endpoint. Required. |
| `--scheme SCHEME` | `http` or `https`. Default: `https`. |
| `--bucket BUCKET` | Bucket name. Required. |
| `--access-key STRING` | AWS access key id. Environment variable: `AWS_ACCESS_KEY_ID`. |
| `--secret-key STRING` | AWS secret key. Environment variable: `AWS_SECRET_ACCESS_KEY`. |
| `--aws-profile STRING` | Named profile in `~/.aws/credentials`. Environment variable: `AWS_PROFILE`. Default: `default`. |
| `--use-virtual-addressing BOOL` | `true` — virtual-hosted-style URL (default). `false` — path-style URL. |
| `--source-prefix PREFIX` | Key prefix of a full backup or one object. Used when `--item` is omitted. |

With `--item`, `source` is a full key prefix in the bucket, same as `ydb import s3`. It is not appended to `--source-prefix`.

### NFS parameters {#nfs}

| Parameter | Description |
| --- | --- |
| `--fs-path PATH` | Directory that contains the backup. Required. Without `--item`, this directory is validated. With `--item`, each `source` is relative to this directory, same as `ydb import nfs`. |

## Result {#result}

A valid backup prints `Backup validation succeeded` and the process exits with code 0. Each problem is printed as `path: message`. When any problem is found, the process exits with code 1.

## Examples {#examples}

Validate a full backup in S3:

```bash
{{ ydb-cli }} tools validate s3 \
  --s3-endpoint storage.example.net --bucket mybucket \
  --source-prefix backup/2026-10-01
```

Validate only one exported table, without reading data bytes:

```bash
{{ ydb-cli }} tools validate s3 \
  --s3-endpoint storage.example.net --bucket mybucket \
  --item source=backup/2026-10-01/dir1/table1 \
  --scheme-only
```

Validate a backup directory on a mounted filesystem:

```bash
{{ ydb-cli }} tools validate nfs --fs-path /mnt/backup/2026-10-01
```

Validate one table under that directory:

```bash
{{ ydb-cli }} tools validate nfs \
  --fs-path /mnt/backup/2026-10-01 \
  --item source=dir1/table1
```

Validate an `--item` export and require a known set of objects. `objects.txt` contains one relative name per line, for example `dir1/table1`:

```bash
{{ ydb-cli }} tools validate nfs \
  --fs-path /mnt/backup/2026-10-01 \
  --expected-objects objects.txt
```
