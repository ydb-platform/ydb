# List of objects

The `scheme ls` command lets you get a list of objects in the database:

```bash
{{ ydb-cli }} [connection options] scheme ls [path] [-lR1]
```

{% include [conn_options_ref.md](conn_options_ref.md) %}

Executing the command without parameters produces a compressed list of object names in the database's root directory.

In the `path` parameter, you can specify the [directory](../dir.md) you want to list objects in.

The following options are available for the command:

* `-l`: Full details about attributes of each object.
* `-R`: Recursive traversal of all subdirectories.
* `-1`: Output a single schema object per row (for example, to be later handled in a script).

**Examples**

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

- Getting objects from the root database directory in a compressed format

```bash
{{ ydb-cli }} --profile quickstart scheme ls
```

- Getting objects in all database directories in a compressed format

```bash
{{ ydb-cli }} --profile quickstart scheme ls -R
```

- Getting objects from the given database directory in a compressed format

```bash
{{ ydb-cli }} --profile quickstart scheme ls dir1
{{ ydb-cli }} --profile quickstart scheme ls dir1/dir2
```

- Getting objects in all subdirectories in the given directory in a compressed format

```bash
{{ ydb-cli }} --profile quickstart scheme ls dir1 -R
{{ ydb-cli }} --profile quickstart scheme ls dir1/dir2 -R
```

- Getting complete information on objects in the root database directory

```bash
{{ ydb-cli }} --profile quickstart scheme ls -l
```

- Getting complete information about objects in a given database directory

```bash
{{ ydb-cli }} --profile quickstart scheme ls dir1 -l
{{ ydb-cli }} --profile quickstart scheme ls dir2/dir3 -l
```

- Getting complete information about objects in all database directories

```bash
{{ ydb-cli }} --profile quickstart scheme ls -lR
```

- Getting complete information on objects in all subdirectories of a given database directory

```bash
{{ ydb-cli }} --profile quickstart scheme ls dir1 -lR
{{ ydb-cli }} --profile quickstart scheme ls dir2/dir3 -lR
```

