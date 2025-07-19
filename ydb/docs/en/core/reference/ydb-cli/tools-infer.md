# Table schema inference from data files

You can use the `{{ ydb-cli }} tools infer csv` command to generate a `CREATE TABLE` statement from a CSV data file. This can be helpful when you want to [import](./export-import/import-file.md) data into a database and the table has not been created yet.

Command syntax:

```bash
{{ ydb-cli }} [global options...] tools infer csv [options...] <input files...>
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
`-p, --path` | Database path to the table that should be created. Default: `table`.
`--columns` | Explicitly specifies table column names, as a comma-separated list.
`--gen-columns` | Explicitly indicates that table column names should be generated automatically (column1, column2, ...).
`--header` | Explicitly indicates that the first row in the CSV contains column names.
`--rows-to-analyze` | Number of rows to analyze. 0 means unlimited. Reading will stop as soon as this number of rows is read. Default: `500000`.
`--execute` | Execute the `CREATE TABLE` request immediately after generation.

{% note info %}

If none of the `--columns`, `--gen-columns`, or `--header` options are explicitly specified, the following algorithm is used:

The values of the first row in the file are checked for the following conditions:

* The values meet the [requirements for column names](../../yql/reference/syntax/create_table/index.md#column-naming-rules).
* The types of the values in the first row are different from the data types in the other rows of the file.

If both conditions are met, the values from the first row are used as the table's column names. Otherwise, column names are generated automatically (as `column1`, `column2`, etc.). See the [example](#example-default) below for more details.

{% endnote %}

## Column type inference algorithm {#column-type-inference}

For each column, the command determines the least general type that fits all its values. The most general type is `Text`: if any value in a column is a string (for example, `abc`), the entire column will be inferred as `Text`.

All integer values are inferred as `Int64` if they fit within the `Int64` range. If any value exceeds this range, the type is set to `Double`.

Floating-point numbers are always inferred as `Double`.

## Current Limitation {#current-limitation}

The first column is always chosen as the primary key. You may need to change the primary key to one that is more appropriate for your use case. For recommendations, see [{#T}](../../dev/primary-key/index.md).

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

### Column names in the first row, no options specified {#example-default}

The `key` and `value` values in the first row match the table column name requirements and do not match the data types in the other rows (`Int64` and `Text`).
So the command uses the first row of the file as column names.

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

The `WITH` block lists some useful table options. Uncomment the ones you need and remove the rest as appropriate.

{% endnote %}

### Column names in the first row, using `--header` option {#example-header}

In this example, the `key` and `value` values in the first row match the data types (`Text`) in the other rows.
In this case, without the `--header` option, the command would not use the first row of the file as column names but would generate column names automatically.
To use the first row as column names in this situation, use the `--header` option explicitly.

```bash
$ cat data_with_header_text.csv
key,value
aaa,bbb
ccc,ddd

{{ ydb-cli }} tools infer csv data_with_header_text.csv --header
CREATE TABLE table (
    key Text,
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

### Explicit column list {#example-columns}

```bash
cat ~/data_no_header.csv
123,abc
456,def

{{ ydb-cli }} tools infer csv -p newtable ~/data_no_header.csv --columns my_key,my_value
CREATE TABLE newtable (
    my_key Int64,
    my_value Text,
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

### Automatically generate column names {#example-gen-columns}

```bash
cat ~/data_no_header.csv
123,abc
456,def

{{ ydb-cli }} tools infer csv -p newtable ~/data_no_header.csv --gen-columns
CREATE TABLE newtable (
    column1 Int64,
    column2 Text,
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

### Executing generated statement using the `--execute` option {#example-execute}

In this example, the `CREATE TABLE` statement is actually executed right after generation.

```bash
$ cat data_with_header.csv
key,value
123,abc
456,def

{{ ydb-cli }} -p quickstart tools infer csv data_with_header.csv --execute
Executing request:

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

Query executed successfully.
```
