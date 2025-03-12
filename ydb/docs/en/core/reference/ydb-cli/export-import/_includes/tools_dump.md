# Exporting data to the file system

## Cluster

The `admin cluster dump` command dumps the cluster' metadata to the client file system in the format described in the [{#T}](../file-structure.md) article:

```bash
{{ ydb-cli }} [connection options] admin cluster dump [options]
```

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

{% include [dump-options.md](./dump-options.md) %}

A [cluster configuration](../../../../maintenance/manual/config-overview.md) is dumped separately using the `{{ ydb-cli }} admin cluster config fetch` command.

## Database

The `admin database dump` command dumps the database' data and metadata to the client file system in the format described in [{#T}](../file-structure.md):

```bash
{{ ydb-cli }} [connection options] admin database dump [options]
```

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

{% include [dump-options.md](./dump-options.md) %}

A [database configuration](../../../../maintenance/manual/config-overview.md) is dumped separately using the `{{ ydb-cli }} admin database config fetch` command.

## Schema objects

The `tools dump` command dumps the schema objects to the client file system in the format described in [{#T}](../file-structure.md):

```bash
{{ ydb-cli }} [connection options] tools dump [options]
```

{% include [conn_options_ref.md](../../commands/_includes/conn_options_ref.md) %}

{% include [dump-options.md](./dump-options.md) %}

- `-p <PATH>` or `--path <PATH>`: Path to the database directory with objects or a path to the table to be dumped. The root database directory is used by default. The dump includes all subdirectories whose names don't begin with a dot and the tables in them whose names don't begin with a dot. To dump such tables or the contents of such directories, you can specify their names explicitly in this parameter.

- `--exclude <STRING>`: Template ([PCRE](https://www.pcre.org/original/doc/html/pcrepattern.html)) to exclude paths from export. Specify this parameter multiple times to exclude more than one template simultaneously.

- `--scheme-only`: Dump only the details of the database schema objects without dumping their data.

- `--consistency-level <VAL>`: The consistency level. Possible options:

  - `database`: A fully consistent dump, with one snapshot taken before starting the dump. Applied by default.
  - `table`: Consistency within each dumped table, taking individual independent snapshots for each table. Might run faster and have less impact on the current workload processing in the database.

- `--avoid-copy`: Do not create a snapshot before dumping. The default consistency snapshot might be inapplicable in some cases (for example, for tables with external blobs).

- `--save-partial-result`: Retain the result of a partial dump. Without this option, dumps that terminate with an error are deleted.

- `--preserve-pool-kinds`: If enabled, the `tools dump` command saves the storage device types specified for column groups of the tables to the dump (see the `DATA` parameter in [{#T}](../../../../yql/reference/syntax/create_table/family.md) for reference). To import such a dump, the same [storage pools](../../../../concepts/glossary.md#storage-pool) must be present in the database. If at least one storage pool is missing, the import procedure will end with an error. By default, this option is disabled, and the import procedure uses the default storage pool specified at the time of database creation (see [{#T}](../../../../devops/manual/initial-deployment.md#create-db) for reference).

- `--ordered`: Sorts rows in the exported tables by the primary key.

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

To an automatically created `backup_...` directory in the current directory:

```bash
{{ ydb-cli }} -e <endpoint> -d <database> admin database dump
```

To a specific directory:

```bash
{{ ydb-cli }} -e <endpoint> -d <database> admin database dump -o ~/backup_db
```

### Exporting a database schema objects

To an automatically created `backup_...` directory in the current directory:

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


