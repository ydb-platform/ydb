# Changing column groups

The mechanism of [column groups](../../../../concepts/datamodel/table.md#column-groups) allows for improved performance of partial row read operations by dividing the storage of table columns into several groups. The most commonly used scenario is to organize the storage of infrequently used attributes into a separate column group.

## Creating column groups {#creating-column-groups}

`ADD FAMILY`: Creates a new group of columns in the table. The code below creates the `family_small` column group in the `series_with_families` table.

```yql
ALTER TABLE series_with_families ADD FAMILY family_small (
    DATA = "ssd",
    COMPRESSION = "off"
);
```

## Modifying column groups {#mod-column-groups}

Using the `ALTER COLUMN` command, you can change a column group for the specified column. The code below for the `release_date` column in the `series_with_families` table changes the column group to `family_small`.

```sql
ALTER TABLE series_with_families ALTER COLUMN release_date SET FAMILY family_small;
```

The two previous commands from listings 8 and 9 can be combined into one `ALTER TABLE` call. The code below creates the `family_small` column group and sets it for the `release_date` column in the `series_with_families` table.

```yql
ALTER TABLE series_with_families
    ADD FAMILY family_small (
        DATA = "ssd",
        COMPRESSION = "off"
    ),
    ALTER COLUMN release_date SET FAMILY family_small;
```

Using the `ALTER FAMILY` command, you can change the parameters of the column group.


### Changing storage type

{% if oss == true and backend_name == "YDB" %}

{% include [OLTP_only_allow_note](../../../../_includes/only_allow_for_oltp_note.md) %}

{% endif %}

The code below changes the storage type to `hdd` for the `default` column group in the `series_with_families` table:

```yql
ALTER TABLE series_with_families ALTER FAMILY default SET DATA "hdd";
```

{% note info %}

Available types of storage devices depend on the {{ ydb-short-name }} cluster configuration.

{% endnote %}

### Changing compression codec

The code below changes the compression codec to `lz4` for the `default` column group in the `series_with_families` table:

```yql
ALTER TABLE series_with_families ALTER FAMILY default SET COMPRESSION "lz4";
```

### Changing compression level of codec

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_only_allow_note](../../../../_includes/only_allow_for_olap_note.md) %}

{% endif %}

The code below changes the compression level of codec if it supports different compression levels for the `default` column group in the `series_with_families` table:

```yql
ALTER TABLE series_with_families ALTER FAMILY default SET COMPRESSION_LEVEL 5;
```

You can specify any parameters of a group of columns from the [`CREATE TABLE`](../create_table/index.md) command.
