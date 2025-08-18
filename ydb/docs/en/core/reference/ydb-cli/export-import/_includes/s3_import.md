# Importing data from an S3 compatible storage

The `import s3` command starts, on the server side, the process of importing data and schema objects details from an S3-compatible storage, in the format described in the [File structure](../file-structure.md) section:

```bash
{{ ydb-cli }} [connection options] import s3 [options]
```

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

As opposed to the [`tools restore` command](../tools-restore.md), the `import s3` command always creates objects in entirety, so none of the imported objects (directories or tables) should already exist.

If you need to import some more data to your existing S3 tables (for example, using [S3cmd](https://s3tools.org/s3cmd)), you can copy the S3 contents to the file system and use the [`tools restore`](../tools-restore.md) command.

## Command line parameters {#pars}

`[options]`: Command parameters:

### S3 parameters {#s3-params}

To run the command to import data from an S3 storage, specify the [S3 connection parameters](../auth-s3.md). As data is imported by the YDB server asynchronously, the specified endpoint must be available so that a connection can be established from the server side.

`--source-prefix PREFIX`: Source prefix for export in the bucket.

### Imported schema objects {#objects}

`--destination-path PATH`: Destination folder for the objects being imported; defaults to the database root if not provided.

`--include PATH`: Schema objects to be included in the import. Directories are traversed recursively. You may specify this parameter multiple times to include several objects. If not specified, all objects in export are imported.

{% cut "Alternate syntax" %}

There's an alternate syntax to specify the list of imported objects, supported for backward compatibility.

`--item STRING`: Description of the item to import. You can specify the `--item` parameter multiple times to import multiple items. If no `--item` or `--include` parameters are specified, all objects from the source prefix will be imported. `STRING` is specified in the `<property>=<value>,...` format with the following properties:

- `source`, `src`, or `s` is the key prefix in S3 that contains the imported directory or table.
- `destination`, `dst`, or `d` is the database path to host the imported directory or table. The destination of the path must not exist. All the directories along the path will be created if missing.

Some features may not be available using the alternate syntax (like encryption and listing).

{% endcut %}

### Additional parameters {#aux}

| Parameter | Description |
--- | ---
| `--description STRING` | A text description of the operation saved in the operation history. |
| `--retries NUM` | The number of import retries to be made by the server. The default value is 10. |
| `--skip-checksum-validation` | Skip the validating imported objects' [checksums](../file-structure.md#checksums) step. |
| `--encryption-key-file PATH` | File path containing the encryption key (only for encrypted exports). The file is binary and must contain exactly the number of bytes matching the key length for the chosen encryption algorithm (16 bytes for `AES-128-GCM`, 32 bytes for `AES-256-GCM` and `ChaCha20-Poly1305`). The key can also be provided using the `YDB_ENCRYPTION_KEY` environment variable, in hexadecimal string representation. |
| `--list` | List objects in an existing export. |
| `--format STRING` | Result format.<br/>Possible values:<br/><ul><li>`pretty`: Human-readable format (default).</li><li>`proto-json-base64`: [Protocol Buffers](https://en.wikipedia.org/wiki/Protocol_Buffers) in [JSON](https://en.wikipedia.org/wiki/JSON) format, binary strings are [Base64](https://en.wikipedia.org/wiki/Base64)-encoded.</li></ul> |

## Importing {#exec}

### Import result {#result}

If successful, the `import s3` command prints summary information about the enqueued operation to import data from S3 in the format specified in the `--format` option. The import itself is performed by the server asynchronously. The summary shows the operation ID that you can use later to check the operation status and perform actions on it:

- In the default `pretty` mode, the operation ID is displayed in the id field with semigraphics formatting:

   ```text
   ┌───────────────────────────────────────────┬───────┬─────...
   | id                                        | ready | stat...
   ├───────────────────────────────────────────┼───────┼─────...
   | ydb://import/8?id=281474976788395&kind=s3 | true  | SUCC...
   ├╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴...
   | Items:
   ...
   ```

- In the `proto-json-base64` mode, the operation ID is in the "id" attribute:

   ```json
   {"id":"ydb://export/8?id=281474976788395&kind=s3","ready":true, ... }
   ```

### Import status {#status}

Data is imported in the background. To get information on import status, use the `operation get` command with the operation ID **enclosed in quotation marks** and passed as a command parameter. For example:

```bash
{{ ydb-cli }} -p quickstart operation get "ydb://import/8?id=281474976788395&kind=s3"
```

The `operation get` format is also set by the `--format` option.

Although the operation ID is in URL format, there is no guarantee that it is maintained in the future. It should only be interpreted as a string.

You can track the import by changes in the "progress" attribute:

- In the default `pretty` mode, successfully completed export operations are displayed as "Done" in the `progress` field with semigraphics formatting:

   ```text
   ┌───── ... ──┬───────┬─────────┬──────────┬─...
   | id         | ready | status  | progress | ...
   ├──────... ──┼───────┼─────────┼──────────┼─...
   | ydb:/...   | true  | SUCCESS | Done     | ...
   ├╴╴╴╴╴ ... ╴╴┴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴┴╴╴╴╴╴╴╴╴╴╴┴╴...
   ...
   ```

- In the `proto-json-base64` mode, the completed export operation is indicated with the `PROGRESS_DONE` value of the `progress` attribute:

   ```json
   {"id":"ydb://...", ...,"progress":"PROGRESS_DONE",... }
   ```

### Completing the import operation {#forget}

When the import is complete, use `operation forget` to delete the import from the operation list:

```bash
{{ ydb-cli }} -p quickstart operation forget "ydb://import/8?id=281474976788395&kind=s3"
```

### List of import operations {#list}

To get a list of import operations, run the `operation list import/s3` command:

```bash
{{ ydb-cli }} -p quickstart operation list import/s3
```

The `operation list` format is also set by the `--format` option.

## Examples {#examples}

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

### Importing to the database root {#example-full-db}

Importing to the database root the contents of the `export1` directory in the `mybucket` bucket using the S3 authentication parameters taken from the environment variables or the `~/.aws/credentials` file:

```bash
{{ ydb-cli }} -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --source-prefix export1
```

### Importing multiple directories {#example-specific-dirs}

Importing items from the `dir1` and `dir2` directories of an export located in `export1` in the `mybucket` S3 bucket to the same-name database directories using explicitly specified S3 authentication parameters:

```bash
{{ ydb-cli }} -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --source-prefix export1
  --include dir1 --include dir2
```

### List objects in existing encrypted export {#example-list}

Listing all object paths in existing encrypted export located in `export1` in the `mybucket` S3 bucket, using the secret key stored in the `~/my_secret_key` file.

```bash
{{ ydb-cli }} -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --source-prefix export1
  --encryption-key-file ~/my_secret_key
  --list
```

### Importing encrypted export {#example-encryption}

Importing one table that was exported using the `dir/my_table` path to the `dir1/dir/my_table` path from an encrypted export located in `export1` in the `mybucket` S3 bucket, using the secret key stored in the `~/my_secret_key` file.

```bash
{{ ydb-cli }} -p quickstart import s3 \
  --s3-endpoint storage.yandexcloud.net --bucket mybucket \
  --access-key <access-key> --secret-key <secret-key> \
  --source-prefix export1 --destination-path dir1 \
  --include dir/my_table \
  --encryption-key-file ~/my_secret_key
```

### Getting operation IDs {#example-list-oneline}

To get a list of import operation IDs in a bash-friendly format, use the [jq](https://stedolan.github.io/jq/download/) utility:

```bash
{{ ydb-cli }} -p quickstart operation list import/s3 --format proto-json-base64 | jq -r ".operations[].id"
```

You'll get a result where each new line shows an operation's ID. For example:

```text
ydb://import/8?id=281474976789577&kind=s3
ydb://import/8?id=281474976789526&kind=s3
ydb://import/8?id=281474976788779&kind=s3
```

You can use these IDs, for example, to run a loop to end all the current operations:

```bash
{{ ydb-cli }} -p quickstart operation list import/s3 --format proto-json-base64 | jq -r ".operations[].id" | while read line; do {{ ydb-cli }} -p quickstart operation forget $line;done
```
