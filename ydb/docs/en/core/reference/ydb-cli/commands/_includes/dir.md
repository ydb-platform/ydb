# Directories

The {{ ydb-short-name }} database supports an internal [directory structure](../../../../concepts/datamodel/dir.md) that can host database objects.

{{ ydb-short-name }} CLI supports operations to change the directory structure and to access schema objects by their directory name.

## Creating a directory {#mkdir}

The `scheme mkdir` command creates the directories:

```bash
{{ ydb-cli }} [connection options] scheme mkdir <path>
```

{% include [conn_options_ref.md](conn_options_ref.md) %}

In the `path` parameter, specify the relative path to the directory being created, from the root database directory. This command creates all the directories that didn't exist at the path when the command was called.

If the destination directory had already existed at the path, then the command execution will be completed successfully (result code 0) with a warning that no changes have been made:

```text
Status: SUCCESS
Issues:
<main>: Error: dst path fail checks, path: /<database>/<path>: path exist, request accepts it,
pathId: [OwnerId: <some>, LocalPathId: <some>], path type: EPathTypeDir, path state: EPathStateNoChanges
```

It also supports the syntax of a full path beginning with `/`. The full path must begin with the [path to the database](../../../../concepts/connect.md#database) specified in the connection parameters or allowed by the current connection to the cluster.

Examples:

- Creating a directory at the database root

   ```bash
   {{ ydb-cli }} --profile quickstart scheme mkdir dir1
   ```

- Creating directories at the specified path from the database root

   ```bash
   {{ ydb-cli }} --profile quickstart scheme mkdir dir1/dir2/dir3
   ```

## Deleting a directory {#rmdir}

The `scheme rmdir` command deletes a directory:

```bash
{{ ydb-cli }} [global options...] scheme rmdir [options...] <path>
```

* `global options`: [Global parameters](../../commands/global-options.md).
* `options`: [Parameters of the subcommand](#rmdir-options).
* `path`: Path to the deleted directory.

Look up the description of the directory deletion command:

```bash
{{ ydb-cli }} scheme rmdir --help
```

### Parameters of the subcommand {#rmdir-options}

| Name | Description |
---|---
| `-r`, `--recursive` | This option deletes the directory recursively, which all its child objects (subdirectories, tables, topics). If you use this option, the confirmation prompt is shown by default. |
| `-f`, `--force` | Do not prompt for confirmation. |
| `-i` | Prompt for deletion confirmation on each object. |
| `-I` | Show a single confirmation prompt. |
| `--timeout <value>` | Operation timeout, ms. |

If you try to delete a non-empty directory without the `-r` or `--recursive` option, the command fails with an error.

```text
Status: SCHEME_ERROR
Issues:
<main>: Error: path table fail checks, path: /<database>/<path>: path has children, request
doesn't accept it, pathId: [OwnerId: <some>, LocalPathId: <some>], path type:
EPathTypeDir, path state: EPathStateNoChanges, alive children: <count>
```

### Examples {#rmdir-examples}

- Deleting an empty directory:

   ```bash
   {{ ydb-cli }} scheme rmdir dir1
   ```

- Deleting an empty directory with a confirmation prompt:

   ```bash
   {{ ydb-cli }} scheme rmdir -I dir1
   ```

- Recursively deleting a non-empty directory with a confirmation prompt:

   ```bash
   {{ ydb-cli }} scheme rmdir -r dir1
   ```

- Recursively deleting a non-empty directory without a confirmation prompt:

   ```bash
   {{ ydb-cli }} scheme rmdir -rf dir1
   ```

- Recursively deleting a non-empty directory, showing a confirmation prompt on each object:

   ```bash
   {{ ydb-cli }} scheme rmdir -ri dir1
   ```

## Using directories in other CLI commands {#use}

In all CLI commands to which the object name is passed by the parameter, it can be specified with a directory, for example, in [`scheme describe`](../scheme-describe.md):

```bash
{{ ydb-cli }} --profile quickstart scheme describe dir1/table_a
```

The [`scheme ls`](../scheme-ls.md) command supports passing the path to the directory as a parameter:

```bash
{{ ydb-cli }} --profile quickstart scheme ls dir1/dir2
```

## Using directories in YQL {#yql}

Names of objects used in [YQL queries](../../../../yql/reference/index.md) may contain paths to directories hosting such objects. This path will be concatenated with the path prefix from the [`TablePathPrefix` pragma](../../../../yql/reference/syntax/pragma.md#table-path-prefix). If the pragma is omitted, the object name is resolved relative to the database root.

## Implicit creation of directories during import {#import}

The data import command creates a directory tree mirroring the original imported catalog.
