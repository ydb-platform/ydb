# Convert a table between row-oriented and column-oriented

{{ ydb-short-name }} supports two main types of tables: [row-oriented](../../concepts/datamodel/table.md#row-oriented-tables) and [column-oriented](../../concepts/datamodel/table.md#column-oriented-tables). The chosen table type determines the physical representation of data on disks, so changing the type in place is impossible. However, you can create a new table of a different type and copy the data. This recipe consists of two steps:

1. [Prepare a new table](#prepare)
2. [Copy data](#copy)

The text below assumes that the source table is row-oriented, and the goal is to convert it to a column-oriented destination table; however, the roles could be reversed.

{% include [ydb-cli-profile.md](../../_includes/ydb-cli-profile.md) %}

## Prepare a new table {#prepare}

Take a copy of the original `CREATE TABLE` statement used for the source table. Modify the following to create a file with the `CREATE TABLE` query for the destination table:

1. Change the table name to an unused name.
2. Add or change the `STORE` setting value to `COLUMN` to make it a column-oriented table.

Run this query (assuming it is saved in a file named `create_column_oriented_table.yql`):

```bash
$ ydb -p quickstart yql -f create_column_oriented_table.yql
```

{% cut "Example test data and table schemas" %}

Row-oriented source table:

```yql
CREATE TABLE `row_oriented_table` (
    id Int64 NOT NULL,
    metric_a Double,
    metric_b Double,
    metric_c Double,
    PRIMARY KEY (id)
)
```

Column-oriented destination table:

```yql
CREATE TABLE `column_oriented_table` (
    id Int64 NOT NULL,
    metric_a Double,
    metric_b Double,
    metric_c Double,
    PRIMARY KEY (id)
)
PARTITION BY HASH(id)
WITH (STORE = COLUMN)
```

Fill the source row-oriented table with random data:

```yql
INSERT INTO `row_oriented_table` (id, metric_a, metric_b, metric_c)
SELECT
    id,
    Random(id + 1),
    Random(id + 2),
    Random(id + 3)
FROM (
    SELECT ListFromRange(1, 1000) AS id
) FLATTEN LIST BY id
```

{% endcut %}

## Copy data {#copy}

Currently, the recommended way to copy data between {{ ydb-short-name }} tables of different types is to export and import:

1. Export data to the local filesystem:

```bash
$ ydb -p quickstart dump -p row_oriented_table -o tmp_backup/
```

2. Import it back into another {{ ydb-short-name }} table:

```bash
ydb -p quickstart import file csv -p column_oriented_table tmp_backup/row_oriented_table/*.csv
```

If the dataset is large, consider using a filesystem that can handle it.

## See also

* [{#T}](../../reference/ydb-cli/index.md)
* [{#T}](../../dev/index.md)