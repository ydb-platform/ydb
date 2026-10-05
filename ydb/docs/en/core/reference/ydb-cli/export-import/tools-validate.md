# Validating a backup

The `tools validate` command validates the integrity of a backup created by [`export s3`](./export-s3.md) or [`export nfs`](./export-nfs.md). The check covers the set of files, the metadata, and the checksums. The command reads the backup files directly from S3 or the file system. It does not load anything into the database and does not require a connection to {{ ydb-short-name }}.

```bash
{{ ydb-cli }} tools validate s3 [options]
{{ ydb-cli }} tools validate nfs [options]
```

The `s3` and `nfs` subcommands differ only in where the backup is located. The backup file format is described in [File structure of data export](./file-structure.md).

{% note warning %}

Successful validation means that the backup files are intact and mutually consistent. It does not guarantee that the backup can be imported: the command neither parses the contents of CSV and Parquet files nor checks them against the table schema.

{% endnote %}

## What can be validated {#layouts}

The command supports three input types, depending on how the backup being validated was created and on the parameters passed to the command.

**A full backup** is the result of running `export s3` with `--destination-prefix` or `export nfs` with `--fs-path`, without the `--item` parameter. Such a backup stores a list of the exported objects, and the command checks every object from that list. Extra files present in the backup but not included in the list are reported as errors.

**A selective backup** is the result of an export that uses `--item src=...,dst=...`. Such a backup does not store a separate list of exported objects. The prefix that holds the selective backup contains directories of exported objects, which the integrity check finds by their metadata files (`scheme.pb` and others). The command checks every object it finds, including index tables, even when they are not listed in the table metadata. Because the list of exported objects is not stored in a selective backup, by default the command cannot check that such a backup is complete: for example, it will not detect an object deleted from the backup by mistake. To check completeness, pass the list of expected object names with the [`--expected-objects`](#expected-objects) parameter.

**A single object** is a path to a directory containing one exported object, such as a table, view, or topic. For a table, the command also checks the indexes and changefeeds listed in its `metadata.json`, as well as the index tables found in its directory.

The command detects the input type automatically. A path is treated as a full backup when it contains the files that hold the object list. Otherwise, the command checks the path as a selective backup or as a single object, depending on its contents. The `--format` parameter sets the input type explicitly:

- `auto` (default) — detect the input type automatically, as described above;
- `full` — check the path as a full backup;
- `item` — check the path as a selective backup. If the contents look like a full backup, the command prints a warning.

## List of expected objects {#expected-objects}

To check whether a selective backup is complete, use `--expected-objects` to specify a text file containing the list of expected objects. Each nonempty line must contain an object name relative to the path being validated, such as `dir1/table1`. The command reports a warning for an object that is present in the backup but absent from the file. It reports an error for an object that is listed in the file but missing from the backup. Index tables inside the directory of a listed object count as part of that object and do not need to be listed separately. For a full backup, `--expected-objects` has no effect because the object list is stored in the backup itself.

Use the `tools list-objects` command to generate this list. It prints the names of the schema objects under the specified database path: tables, column-oriented tables, views, and topics. Each name is printed on its own line, in the format accepted by `--expected-objects`:

```bash
{{ ydb-cli }} [connection options] tools list-objects [options]
```

Unlike `tools validate`, this command requires a connection to the database.

| Parameter | Description |
| --- | --- |
| `-p`, `--path PATH` | Path to a directory or an object in the database, either relative to the database root or a full path that starts with the database path. Default: `.`, the database root. |
| `--include-index-data` | Also list the index tables that an export with `--include-index-data` writes as separate objects. |
| `-o`, `--output PATH` | Write the list to a file. By default, the list is printed to standard output. |

Names are printed relative to `--path`. If `--path` points to a single table, the output is `.`, and its index tables (with `--include-index-data`) are printed as `index_name/indexImplTable`.

## What is checked {#checks}

Metadata files, including `scheme.pb`, `permissions.pb`, `metadata.json`, changefeed and topic descriptions, SQL and protobuf schema files, and files in `SchemaMapping`, must be readable and contain the required minimum set of fields that describe the object. For a table, this means a non-empty column list, a type for every column, and a primary key made up of columns from that list.

The number of `data_...` files must match the partition count set in `scheme.pb`. `uniform_partitions` specifies the partition count directly. With `partition_at_keys`, the partition count is the number of `split_points` plus one. If neither field is set, the table has one partition and must contain a single data file, `data_00`. File numbers must run from `0` to `N-1` with no gaps, duplicates, or extra files. File names use forms such as `data_00.csv` and `data_01.parquet`. The sequence number must contain at least two digits, and compressed files have an additional `.zst` suffix.

Each checksum file is stored next to the file it covers. For both `data_00.csv` and its compressed form, `data_00.csv.zst`, the checksum file is named `data_00.csv.sha256`. It holds the SHA-256 checksum of the uncompressed contents. Checksums of metadata and schema files are always verified. Unless `--scheme-only` is set, the command also verifies data checksums. It reads each data file as a stream, decompresses Zstandard-compressed data when necessary, and computes the hash incrementally. With `--scheme-only`, data files are not read; the command checks only that each file has a checksum file containing a SHA-256 checksum encoded in hexadecimal.

Data checksums can be verified only for backups whose checksums were computed during export. Starting with version 25.3, checksums are computed by default. Such backups have `"checksum": "sha256"` in the backup `metadata.json` and `"version": 1` in the object metadata. If the checksum files are absent, validation without `--scheme-only` fails.

The command does not validate encrypted backups. It reports `.enc` files and the `encryption` field in the backup's `metadata.json` as errors. `--encryption-key-file` is accepted only for compatibility with the import commands; `tools validate` does not use the key. If a key is provided but the backup contains no encrypted files, the command prints a warning.

After an I/O error, the command retries the read, gradually increasing the delay between attempts from 100 ms to 2 s. The number of attempts is set by `--retries`.

## Errors and warnings {#errors}

By default, the command continues after an error and reports every problem it finds. `--fail-fast` stops validation after the first error: the remaining `--item` paths are not checked, but checks already running in other threads continue to completion. Warnings neither stop validation nor affect the exit code.

## Command line parameters {#pars}

### Common parameters {#common}

| Parameter | Description |
| --- | --- |
| `--format FORMAT` | Expected [input type](#layouts): `auto`, `full`, or `item`. Default: `auto`. |
| `--scheme-only` | Check only the set of files, the metadata structure, and the presence of checksum files. Data files are not read. |
| `--fail-fast` | Stop at the first error. By default, every error is reported. |
| `--threads NUM` | Maximum number of threads. Different objects, as well as the data files of a single object, are checked in parallel. Default: one less than the number of available processors, with a minimum of one. |
| `--retries NUM` | Number of attempts to read a backup file on I/O errors. Default: `10`. |
| `--encryption-key-file PATH` | Accepted for compatibility with [`import s3`](./import-s3.md) and [`import nfs`](./import-nfs.md); the key is not used. As with those commands, the key can be passed as a hexadecimal string in the `YDB_ENCRYPTION_KEY` environment variable, or as a file path in `YDB_ENCRYPTION_KEY_FILE`. Encrypted files are reported as errors regardless. |
| `--item PROPERTY=VALUE,...` | Object to check; repeat this parameter to check multiple objects. The `source` property (aliases: `src`, `s`) specifies the path to the backup or object. The `destination` property (aliases: `dst`, `d`) is accepted for compatibility with the import commands and is ignored. |
| `--expected-objects PATH` | File with the [list of expected objects](#expected-objects) for a selective backup. |

### S3 parameters {#s3}

Connection parameters are the same as for [`import s3`](./import-s3.md); see [Connecting to and authenticating with S3](./auth-s3.md).

| Parameter | Description |
| --- | --- |
| `--s3-endpoint ENDPOINT` | S3 endpoint. Required. |
| `--scheme SCHEME` | `http` or `https`. Default: `https`. |
| `--bucket BUCKET` | Bucket name. Required. |
| `--access-key STRING` | AWS access key ID. Environment variable: `AWS_ACCESS_KEY_ID`. |
| `--secret-key STRING` | AWS secret key. Environment variable: `AWS_SECRET_ACCESS_KEY`. |
| `--aws-profile STRING` | Profile name in `~/.aws/credentials`. Environment variable: `AWS_PROFILE`. Default: `default`. |
| `--use-virtual-addressing BOOL` | Bucket addressing style: `true` — virtual-hosted-style (default), `false` — path-style. |
| `--source-prefix PREFIX` | Key prefix of a full backup or a single object. Used when `--item` is not set. |

With `--item`, the `source` property specifies a full key prefix in the bucket, as it does for `import s3`. The value of `--source-prefix` is not prepended.

### NFS parameters {#nfs}

| Parameter | Description |
| --- | --- |
| `--fs-path PATH` | Directory that contains the backup. Required. Without `--item`, the command checks the directory itself. With `--item`, the `source` property of each item is relative to this directory, as it is for `import nfs`. |

## Result {#result}

Each error and warning is printed to standard error on a separate line as `path: message`; warning lines are prefixed with `warning:`. A summary line is printed at the end:

- `Backup validation succeeded: checked N object(s)` — no errors; exit code `0`. If there were warnings, their number is included in the same line.
- `Backup validation failed: N issue(s), checked M object(s)` — at least one error was found; exit code `1`.

## Examples {#examples}

Validate a full backup in S3:

```bash
{{ ydb-cli }} tools validate s3 \
  --s3-endpoint storage.example.net --bucket mybucket \
  --source-prefix backup/2026-10-01
```

Validate a single exported table without reading the data files:

```bash
{{ ydb-cli }} tools validate s3 \
  --s3-endpoint storage.example.net --bucket mybucket \
  --item source=backup/2026-10-01/dir1/table1 \
  --scheme-only
```

Validate a backup on a mounted file system:

```bash
{{ ydb-cli }} tools validate nfs --fs-path /mnt/backup/2026-10-01
```

Validate a single table from that backup:

```bash
{{ ydb-cli }} tools validate nfs \
  --fs-path /mnt/backup/2026-10-01 \
  --item source=dir1/table1
```

Check whether a backup created with `--item` is complete. First, use `tools list-objects` to save the list of objects under the database directory `dir1` to `objects.txt`. The command uses the `quickstart` profile to connect to the database (see [{#T}](../profile/create.md#quickstart)). Then use `tools validate` to compare the backup with that list:

```bash
{{ ydb-cli }} -p quickstart tools list-objects --path dir1 --output objects.txt
{{ ydb-cli }} tools validate nfs \
  --fs-path /mnt/backup/2026-10-01/dir1 \
  --expected-objects objects.txt
```
