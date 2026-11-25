# Validating a backup

The `tools validate` command checks the byte-level integrity of a backup or exported schema object created by [`export s3`](./export-s3.md) or [`export nfs`](./export-nfs.md). It checks the file layout, metadata consistency, and checksums. The command reads the exported files directly from S3 or the file system without loading data into a database, so it does not require a connection to {{ ydb-short-name }}.

```bash
{{ ydb-cli }} tools validate s3 [options]
{{ ydb-cli }} tools validate nfs [options]
```

The `s3` and `nfs` subcommands differ only in the location of the exported files. They use the same file format and directory structure, described in [File structure of data export](./file-structure.md).

{% note warning %}

Successful validation means that the exported files are intact and mutually consistent. It does not guarantee that they can be imported: the command does not parse rows in CSV or Parquet files or check their values against the table schema.

{% endnote %}

## Supported export layouts {#layouts}

The command supports the following inputs:

- **A full backup** created without `--item`, using `--destination-prefix` for `export s3` or `--fs-path` for `export nfs`. The command checks every object in the backup's object list and reports unlisted schema objects as errors.
- **An item-style export** created with `--item src=...,dst=...`. This layout does not contain a complete object list. The command checks every supported schema object it finds, including index tables. To detect objects missing from the export, provide the expected object list with [`--expected-objects`](#expected-objects).
- **A single exported object**, such as a table, view, or topic. For a table, the command also checks its exported indexes and changefeeds.

By default, the command detects the layout automatically. Use `--format` to select it explicitly:

- `auto` (default) — detect a full backup; otherwise, check an item-style export or a single object;
- `full` — require a full backup;
- `item` — treat the path as an item-style export or a single object. If the path looks like a full backup, the command prints a warning and does not check the full backup's object list.

## List of expected objects {#expected-objects}

To check whether an item-style export is complete, use `--expected-objects` to specify a text file containing the expected objects. Each nonempty line must contain an object name relative to the path being validated, such as `dir1/table1`. An object found in the export but absent from the file produces a warning. An object listed in the file but missing from the export produces an error. Index tables inside a listed object's directory do not need to be listed separately.

Do not use `--expected-objects` with a full backup: a full backup already contains an object list, and combining the two results in an error.

Use `tools list-objects` to generate the file. It lists row and column tables, views, and topics under a database path. Each object name is printed on a separate line in the format accepted by `--expected-objects`:

```bash
{{ ydb-cli }} [connection options] tools list-objects [options]
```

Unlike `tools validate`, this command requires a connection to the database.

| Parameter | Description |
| --- | --- |
| `-p`, `--path PATH` | Path to a directory or an object in the database, either relative to the database root or a full path that starts with the database path. Default: `.`, the database root. |
| `--include-index-data` | Also list the index tables exported as separate objects when `--include-index-data` is used. |
| `-o`, `--output PATH` | Write the list to a file. By default, the list is printed to standard output. |

Names are relative to `--path`. If `--path` points to a single table, the table is listed as `.`.

## What is checked {#checks}

The command checks:

- that the required schema, metadata, and data files are present and readable;
- that metadata has a supported structure and is internally consistent;
- that the number and format of table data files match the table metadata;
- that the listed or discovered schema objects and their related indexes and changefeeds are consistent;
- that required checksum files are present and valid.

During a full check, the command reads each data file and compares the SHA-256 checksum of its uncompressed contents with the exported checksum. If data checksums are absent, the command cannot confirm content integrity and reports an error. With `--scheme-only`, data file contents are not read; only the file layout, metadata, and checksum files are checked.

The `--metadata-checksums` parameter controls whether checksum files for `metadata.json` are required. The default `auto` mode follows the checksum information in the export. Use `always` to require these checksum files even for older exports, or `ignore` to skip checking them.

Encrypted backups are not supported. Encrypted files are reported as errors even if an encryption key is provided.

## Errors and warnings {#errors}

By default, the command continues after an error and reports every problem it finds. `--fail-fast` stops scheduling checks after the first error; checks that have already started can still finish. Warnings do not stop validation or affect the exit code.

## Command line parameters {#pars}

### Common parameters {#common}

| Parameter | Description |
| --- | --- |
| `--format FORMAT` | Expected [input type](#layouts): `auto`, `full`, or `item`. Default: `auto`. |
| `--scheme-only` | Check the file layout, metadata, and checksum files without reading data file contents. |
| `--metadata-checksums MODE` | How to check `metadata.json` checksums: `auto` follows the export's checksum information, `always` requires a checksum file for every `metadata.json`, and `ignore` skips metadata checksum validation. Default: `auto`. |
| `--fail-fast` | Stop at the first error. By default, every error is reported. |
| `--threads NUM` | Maximum number of validation threads. Default: one less than the number of available processors, with a minimum of one. |
| `--retries NUM` | Number of attempts to read a backup file on I/O errors. Default: `10`. |
| `--encryption-key-file PATH` | Accepted for compatibility with [`import s3`](./import-s3.md) and [`import nfs`](./import-nfs.md), but the key is not used. Encrypted files are reported as errors. The corresponding environment variables are `YDB_ENCRYPTION_KEY_FILE` and `YDB_ENCRYPTION_KEY`. |
| `--item PROPERTY=VALUE,...` | Object to check; repeat this parameter to check multiple objects. The `source` property (aliases: `src`, `s`) specifies the path to the backup or object. The `destination` property (aliases: `dst`, `d`) is accepted for compatibility with the import commands and is ignored. |
| `--expected-objects PATH` | File with the [list of expected objects](#expected-objects) for an item-style export. |

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
| `--source-prefix PREFIX` | Key prefix of a full backup, item-style export, or single object. Required unless `--item` is set. |

With `--item`, the `source` property specifies a full key prefix in the bucket, as it does for `import s3`. The value of `--source-prefix` is not prepended.

### NFS parameters {#nfs}

| Parameter | Description |
| --- | --- |
| `--fs-path PATH` | Directory that contains the exported files. Required. Without `--item`, the command checks the directory itself. With `--item`, the `source` property of each item is relative to this directory, as it is for `import nfs`. |

## Result {#result}

Progress and diagnostics are printed to standard error. Validation errors identify the affected path in the form `path: message`, and warning lines start with `warning:`. The command then prints a summary:

- `Backup validation succeeded: checked N object(s)` — no errors; exit code `0`. If there were warnings, their number is included in the same line.
- `Backup validation failed: N issue(s), checked M object(s)` — at least one error was found; exit code `1`.

The success summary is printed to standard output; the failure summary is printed to standard error. A second line shows the elapsed time, total amount of data read, and average read speed.

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
