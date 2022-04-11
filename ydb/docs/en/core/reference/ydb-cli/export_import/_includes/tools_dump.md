# Exporting data to the file system

The `tools dump` command dumps data and information about data schema objects to the client file system in the format described in the [File structure](../file_structure.md) article:

```bash
{{ ydb-cli }} [connection options] tools dump [options]
```

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

`[options]`: Command parameters:

`-p PATH` or `--path PATH`: Path to the DB directory whose objects are to be dumped or path to the table. By default, the DB root directory. The following will be dumped: all subdirectories whose names do not begin with a dot and tables whose names do not begin with a dot inside these subdirectories. To dump such tables or the contents of such directories, you can explicitly specify their names in this parameter.

`-o PATH` or `--output PATH`: Path to the directory in the client file system that data should be dumped to. If the specified directory doesn't exist, it will be created. Anyway, the entire path to it must exist. If the specified directory does exist, it must be empty. If the parameter is not specified, a directory with a name in `backup_YYYYDDMMTHHMMSS` format will be created in the current directory, where YYYYDDMM indicates the date and HHMMSS the export start time.

`--exclude STRING`: Pattern ([PCRE](https://www.pcre.org/original/doc/html/pcrepattern.html)) for excluding paths from the export destination. This parameter can be specified several times for different patterns.

`--scheme-only`: Only dump information about data schema objects and no data.

`--consistency-level VAL`: Consistency level. Possible options:

- `database`: Fully consistent export with a single snapshot taken before starting the export operation. Applied by default.
- `table`: Consistency within each table being dumped with separate independent snapshots taken for each such table. It can run faster and have less impact on handling the current DB load.

`--avoid-copy`: Do not create a dump snapshot. The snapshot used by default to ensure consistency may not be applicable in some cases (such as for tables with external blobs).

`--save-partial-result`: Do not delete the result of a partially completed dump. If this option is not enabled, the result will be deleted in case an error occurs when dumping data.

## Examples

{% include [example_db1.md](../../_includes/example_db1.md) %}

### Exporting a database

With a directory named `backup_...` automatically created in the current directory:

```
{{ ydb-cli }} --profile db1 tools dump 
```

To the specified directory:

```
{{ ydb-cli }} --profile db1 tools dump -o ~/backup_db1
```

### Exporting the structure of tables in the specified DB directory and its subdirectories

```
{{ ydb-cli }} --profile db1 tools dump -p dir1 --scheme-only
```

