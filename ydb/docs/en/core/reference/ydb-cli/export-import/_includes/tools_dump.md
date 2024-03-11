# Exporting data to the file system

The `tools dump` command dumps the database data and objects schema to the client file system, in the format described in the [File system](../file-structure.md):

```bash
{{ ydb-cli }} [connection options] tools dump [options]
```

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

`[options]`: Command parameters:

`-p PATH` or `--path PATH`: Path to the database directory with objects or a path to the table to be dumped. The root database directory is used by default. The dump includes all subdirectories whose names don't begin with a dot and the tables in them whose names don't begin with a dot. To dump such tables or the contents of such directories, you can specify their names explicitly in this parameter.

`-o PATH` or `--output PATH`: Path to the directory in the client file system to dump the data to. If such a directory doesn't exist, it will be created. The entire path to it must already exist, however. If the specified directory exists, it must be empty. If the parameter is omitted, a directory with the name `backup_YYYYDDMMTHHMMSS` will be created in the current directory, with YYYYDDMM being the date and HHMMSS: the time when the dump began.

`--exclude STRING`: Template ([PCRE](https://www.pcre.org/original/doc/html/pcrepattern.html)) to exclude paths from export. Specify this parameter multiple times for different templates.

`--scheme-only`: Dump only the details about the database schema objects, without dumping their data

`--consistency-level VAL`: The consistency level. Possible options:
- `database`: A fully consistent dump, with one snapshot taken before starting dumping. Applied by default.
- `table`: Consistency within each dumped table, taking individual independent snapshots for each table dumped. Might run faster and have a smaller effect on the current workload processing in the database.

`--avoid-copy`: Do not create a snapshot before dumping. The consistency snapshot taken by default might be inapplicable in some cases (for example, for tables with external blobs).

`--save-partial-result`: Don't delete the result of partial dumping. Without this option, the dumps that terminated with an error are deleted.

`--ordered`: Rows in the exported tables will be sorted by the primary key.

## Examples

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

### Exporting a database

With automatic creation of the `backup_...` directory In the current directory:

```
{{ ydb-cli }} --profile quickstart tools dump
```

To a specific directory:

```
{{ ydb-cli }} --profile quickstart tools dump -o ~/backup_quickstart
```

### Dumping the table structure within a specified database directory (including subdirectories)

```
{{ ydb-cli }} --profile quickstart tools dump -p dir1 --scheme-only
```


