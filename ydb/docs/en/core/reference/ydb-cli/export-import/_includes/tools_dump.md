# Exporting data to the file system

## Cluster

The `admin cluster dump` command dumps the cluster to the client file system, in the format described in the [File system](../file-structure.md):

```bash
{{ ydb-cli }} [connection options] admin cluster dump [options]
```

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

`[options]`: Command parameters:

`-o PATH` or `--output PATH`: Path to the directory in the client file system to dump the data to. If such a directory doesn't exist, it will be created. The entire path to it must already exist, however. If the specified directory exists, it must be empty. If the parameter is omitted, a directory with the name `backup_YYYYDDMMTHHMMSS` will be created in the current directory, with YYYYDDMM being the date and HHMMSS: the time when the dump began.

[Cluster configuration](../../../../maintenance/manual/config-overview.md) is dumped separately using the following steps:

1) Copy the static configuration from the nodes' local disks.
2) Fetch the dynamic configuration using the `{{{ ydb-cli }} admin cluster config fetch` command.

## Database

The `admin database dump` command dumps the database to the client file system, in the format described in the [File system](../file-structure.md):

```bash
{{ ydb-cli }} [connection options] admin database dump [options]
```

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

`[options]`: Command parameters:

`-o PATH` or `--output PATH`: Path to the directory in the client file system to dump the data to. If such a directory doesn't exist, it will be created. The entire path to it must already exist, however. If the specified directory exists, it must be empty. If the parameter is omitted, a directory with the name `backup_YYYYDDMMTHHMMSS` will be created in the current directory, with YYYYDDMM being the date and HHMMSS: the time when the dump began.

[Database configuration](../../../../maintenance/manual/config-overview.md) is dumped separately using the following steps:

1) Copy the static configuration from the nodes' local disks.
2) Fetch the dynamic configuration using the `{{{ ydb-cli }} admin database config fetch` command.

## Schema objects

The `tools dump` command dumps the schema objects to the client file system, in the format described in the [File system](../file-structure.md):

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

`--preserve-pool-kinds`: If this option is enabled, the `tools dump` command saves storage device types specified for column groups of the tables to the dump (see the `DATA` parameter in [Column groups](https://ydb.tech/docs/en/yql/reference/syntax/create_table/family) for the reference). To import such a dump, the same [storage pools](https://ydb.tech/docs/en/concepts/glossary#storage-pool) must be present in the database. If at least one storage pool is missing, the import procedure will end with an error. By default this option is disabled, and the import procedure will use the default storage pool that was specified at the moment of database creation (see [Creating a database](https://ydb.tech/docs/en/devops/manual/initial-deployment#create-db) for the reference).

`--ordered`: Rows in the exported tables will be sorted by the primary key.

## Examples

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

### Exporting a cluster

With automatic creation of the `backup_...` directory in the current directory:

```bash
{{ ydb-cli }} -e <endpoint> admin cluster dump
```

To a specific directory:

```bash
{{ ydb-cli }} -e <endpoint> admin cluster dump -o ~/backup_cluster
```

### Exporting a database

With automatic creation of the `backup_...` directory in the current directory:

```bash
{{ ydb-cli }} -e <endpoint> -d <database> admin database dump
```

To a specific directory:

```bash
{{ ydb-cli }} -e <endpoint> -d <database> admin database dump -o ~/backup_db
```

### Exporting a database schema objects

With automatic creation of the `backup_...` directory in the current directory:

```bash
{{ ydb-cli }} --profile quickstart tools dump
```

To a specific directory:

```bash
{{ ydb-cli }} --profile quickstart tools dump -o ~/backup_quickstart
```

### Dumping the table structure within a specified database directory (including subdirectories)

```bash
{{ ydb-cli }} --profile quickstart tools dump -p dir1 --scheme-only
```


