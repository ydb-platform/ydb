# Directories

The {{ ydb-short-name }} database maintains an internal hierarchical structure of [directories](../../../../concepts/datamodel/dir.md) that can host database objects.

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

The full path syntax starting with a `/` character is also supported. The full path must begin with the [database location](../../../../concepts/connect.md#database) specified in the connection parameters or with which operations are allowed via the established connection to the cluster.

Examples:

- Creating a directory at the database root

  ```bash
  {{ ydb-cli }} --profile db1 scheme mkdir dir1
  ```

- Creating directories at the specified path from the database root

  ```bash
  {{ ydb-cli }} --profile db1 scheme mkdir dir1/dir2/dir3
  ```

## Deleting a directory {#rmdir}

The `scheme rmdir` command deletes a directory:

```bash
{{ ydb-cli }} [connection options] scheme rmdir <path>
```

{% include [conn_options_ref.md](conn_options_ref.md) %}

In the `path` parameter, specify the relative path to the directory to be deleted. This directory must not contain objects (including tables and subdirectories), otherwise the command will fail with an error:

```text
Status: SCHEME_ERROR
Issues: 
<main>: Error: path table fail checks, path: /<database>/<path>: path has children, request 
doesn't accept it, pathId: [OwnerId: <some>, LocalPathId: <some>], path type: 
EPathTypeDir, path state: EPathStateNoChanges, alive children: <count>
```

## Using directories in other CLI commands {#use}

In all CLI commands to which the object name is passed by the parameter, it can be specified with a directory, for example, in [`scheme describe`](../scheme-describe.md):

```bash
{{ ydb-cli }} --profile db1 scheme describe dir1/table_a
```

The [`scheme ls`](../scheme-ls.md) command supports passing the path to the directory as a parameter:

```bash
{{ ydb-cli }} --profile db1 scheme ls dir1/dir2
```

## Using directories in YQL {#yql}

Names of objects issued in [YQL](../../../../yql/reference/index.md) queries may contain a path to the object directory. This path will be concatenated with the path prefix from the [`TablePathPrefix` pragma](../../../../yql/reference/syntax/pragma.md#table-path-prefix). If the pragma is omitted, the object name is resolved relative to the database root.

## Implicit creation of directories during import {#import}

The data import command creates a directory tree mirroring the original imported catalog.

