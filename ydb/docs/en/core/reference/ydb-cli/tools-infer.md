# Table schema inference from data files

You can use the subcommand `{{ ydb-cli }} tools infer csv` to generate a `CREATE TABLE` statement from a CSV data file. This can be very helpful when you want to [import](./export-import/import-file.md) data into a database and the table has not been created yet.

Command syntax:

```bash
{{ ydb-cli }} [global options...] tools infer csv [options...]
```

- `global options` – [global options](commands/global-options.md).
- `options` – [subcommand options](#options).

To get the most up-to-date information about the command, use the `--help` option:

```bash
{{ ydb-cli }} tools infer csv --help
```

## Subcommand options {#options}

Option Name | Description
---|---
`-p, --path` | Database path to table that should be created. Default: `table`.
`--columns` | Explicitly specifies table column names, as a comma-separated list.
`--gen-columns` | Explicitly indicates that table column names should be generated automatically.
`--header` | Explicitly indicates that the first row in the CSV contains column names.
`--rowsalyze` | Number of rows to analyze. 0 means unlimited. Reading will be stopped soon after this number of rows is read. Default: `500000`.
`--execute` | Execute `CREATE TABLE` request right after generation.

{% note info %}

By default, if possible, the command attempts to use the first row of the file as column names.

Use the options `--columns`, `--gen-columns`, or `--header` to explicitly specify the source of column names.

{% endnote %}

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

### Column names in the first row

```bash
$ cat data_with_header.csv
key,value
123,abc
456,def

{{ ydb-cli }} tools infer csv data_with_header.csv
CREATE TABLE table (
    key Int64,
    value Text,
    PRIMARY KEY (key) -- First column is chosen. Probably need to change this.
)
WITH (
    STORE = ROW -- or COLUMN
    -- Other useful table options to consider:
    --, AUTO_PARTITIONING_BY_SIZE = ENABLED
    --, AUTO_PARTITIONING_BY_LOAD = ENABLED
    --, UNIFORM_PARTITIONS = 100 -- Initial number of partitions
    --, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 100
    --, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000
);
```

{% note info %}

The WITH block lists some useful table options. Uncomment the ones you need, and remove the rest as appropriate.

{% endnote %}

### Explicit column list

```bash
cat ~/data_no_header.csv
123,abc
456,def

{{ ydb-cli }} tools infer csv -p newtable ~/data_no_header.csv --columns my_key,my_value
CREATE TABLE newtable (
    my_key Int64,
    my_value Utf8,
    PRIMARY KEY (my_key)
)
WITH (
    STORE = ROW -- or COLUMN
    -- Other useful table options to consider:
    --, AUTO_PARTITIONING_BY_SIZE = ENABLED
    --, AUTO_PARTITIONING_BY_LOAD = ENABLED
    --, UNIFORM_PARTITIONS = 100 -- Initial number of partitions
    --, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 100
    --, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000
);
```

### Automatically generate column names

```bash
cat ~/data_no_header.csv
123,abc
456,def

{{ ydb-cli }} tools infer csv -p newtable ~/data_no_header.csv --gen-columns
CREATE TABLE newtable (
    f0 Int64,
    f1 Text,
    PRIMARY KEY (f0) -- First column is chosen. Probably need to change this.
)
WITH (
    STORE = ROW -- or COLUMN
    -- Other useful table options to consider:
    --, AUTO_PARTITIONING_BY_SIZE = ENABLED
    --, AUTO_PARTITIONING_BY_LOAD = ENABLED
    --, UNIFORM_PARTITIONS = 100 -- Initial number of partitions
    --, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 100
    --, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000
);
```
