# Working with directories

Create a tree from directories:

```bash
{{ ydb-cli }} scheme mkdir my-directory
```

```bash
{{ ydb-cli }} scheme mkdir my-directory/sub-directory1
```

```bash
{{ ydb-cli }} scheme mkdir my-directory/sub-directory1/sub-directory1-1
```

```bash
{{ ydb-cli }} scheme mkdir my-directory/sub-directory2
```

To view a recursive listing of all subdirectories and their objects at the specified path, use the `-R` option for the `scheme ls` subcommand:

```bash
{{ ydb-cli }} scheme ls my-directory -lR
```

