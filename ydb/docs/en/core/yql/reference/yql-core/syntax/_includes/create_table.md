# CREATE TABLE

{% if feature_olap_tables %}

{{ ydb-short-name }} supports two types of tables:

* [Row-oriented](../../../../concepts/datamodel/table.md)
* [Column-oriented](../../../../concepts/datamodel/table.md#olap-data-types).

When you create a table, the table type is specified by the `STORE` parameter, with `ROW` creating a [row-oriented table](#row) and `COLUMN` creating a [column](#olap-tables)-oriented table. If the `STORE` parameter is omitted, a row-oriented table is created by default.

{% endif %}

{% if feature_olap_tables %}

## Row-oriented tables {#row}

{% endif %}

{% if feature_bulk_tables %}

The table is created automatically during the first [INSERT INTO](insert_into.md){% if feature_mapreduce %} in the database specified in [USE](../use.md){% endif %}. The schema is determined automatically.

{% else %}

The `CREATE TABLE` call creates a {% if concept_table %}[table]({{ concept_table }}){% else %}table{% endif %} with the specified data schema{% if feature_map_tables %} and key columns (`PRIMARY KEY`){% endif %}. {% if feature_secondary_index == true %}It lets you define secondary indexes on the created table.{% endif %}

    CREATE [TEMPORARY | TEMP] TABLE table_name (
        column1 type1,
{% if feature_not_null == true %}        column2 type2 NOT NULL,{% else %}        column2 type2,{% endif %}
        ...
        columnN typeN,
{% if feature_secondary_index == true %}
        INDEX index1_name GLOBAL ON ( column ),
        INDEX index2_name GLOBAL ON ( column1, column2, ... ),
{% endif %}
{% if feature_map_tables %}
        PRIMARY KEY ( column, ... ),
        FAMILY column_family ( family_options, ... )
{% else %}
        ...
{% endif %}
    )
{% if feature_map_tables %}
    WITH ( key = value, ... )
{% endif %}

{% if feature_olap_tables %}#{% endif %}## Columns {#row-columns}

{% if feature_column_container_type == true %}
In non-key columns, you can use any data types, but for key columns, only [primitive ones](../../types/primitive.md). When specifying complex types (for example, `List<String>`), the type is enclosed in double quotes.
{% else %}
For the key and non-key columns, you can only use [primitive](../../types/primitive.md) data types.
{% endif %}

{% if feature_not_null == true %}
Without additional modifiers, a column gets an [optional type](../../types/optional.md) and allows `NULL` values to be written. To create a non-optional type, use `NOT NULL`.
{% else %}
{% if feature_not_null_for_pk %}
All columns are [optional](../../types/optional.md) by default and can be assigned NULL values. The `NOT NULL` limit can only be specified for columns that are part of the primary key..
{% else %}
All columns allow writing `NULL` values, that is, they are [optional](../../types/optional.md).
{% endif %}
{% endif %}
{% if feature_map_tables %}
It is mandatory to specify the `PRIMARY KEY` with a non-empty list of columns. Those columns become part of the key in the listed order.
{% endif %}

**Example**

    CREATE TABLE my_table (
{% if feature_not_null_for_pk %}        a Uint64 NOT NULL,{% else %}        a Uint64,{% endif %}
        b Bool,
{% if feature_not_null %}        c Float NOT NULL,{% else %}        c Float,{% endif %}
{% if feature_column_container_type %}         d "List<List<Int32>>"{% endif %}
{% if feature_map_tables %}
        PRIMARY KEY (b, a)
{% endif %}
    )


{% if feature_secondary_index %}
{% if feature_olap_tables %}#{% endif %}## Secondary indexes {#secondary_index}

The INDEX construct is used to define a {% if concept_secondary_index %}[secondary index]({{ concept_secondary_index }}){% else %}secondary index{% endif %} in a table:

```sql
CREATE TABLE table_name (
    ...
    INDEX <index_name> GLOBAL [SYNC|ASYNC] ON ( <index_columns> ) COVER ( <cover_columns> ),
    ...
)
```

Where:
* **Index_name** is the unique name of the index to be used to access data.
* **SYNC/ASYNC** indicates synchronous/asynchronous data writes to the index. If not specified, synchronous.
* **Index_columns** is a list of comma-separated names of columns in the created table to be used for a search in the index.
* **Cover_columns** is a list of comma-separated names of columns in the created table, which will be stored in the index in addition to the search columns, making it possible to fetch additional data without accessing the table for it.

**Example**

```sql
CREATE TABLE my_table (
    a Uint64,
    b Bool,
    c Utf8,
    d Date,
    INDEX idx_d GLOBAL ON (d),
    INDEX idx_ba GLOBAL ASYNC ON (b, a) COVER (c),
    PRIMARY KEY (a)
)
```
{% endif %}

{% if feature_temp_tables %}
{% if feature_olap_tables %}#{%endif%}## Creating a temporary table {#temporary_tables}
```sql
CREATE TEMPORARY TABLE table_name (
    ...
);
```

{% include [temp-table-description.md](../../../../_includes/temp-table-description.md) %}

{% endif %}

{% if feature_map_tables and concept_table %}
{% if feature_olap_tables %}#{% endif %}## Additional parameters {#row-additional}

You can also specify a number of {{ backend_name }}-specific parameters for the table. When you create a table, those parameters are listed in the ```WITH``` clause:

```sql
CREATE TABLE table_name (...)
WITH (
    key1 = value1,
    key2 = value2,
    ...
)
```

Here, key is the name of the parameter and value is its value.

For a list of valid parameter names and values, see the [{{ backend_name }} table description]({{ concept_table }}).

For example, this code will create a table with enabled automatic partitioning by partition size and the preferred size of each partition is 512 MB:

<small>Listing 4</small>

```sql
CREATE TABLE my_table (
    id Uint64,
    title Utf8,
    PRIMARY KEY (id)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 512
);
```

{% if feature_olap_tables %}#{% endif %}## Column groups {#column-family}

Columns of the same table can be grouped to set the following parameters:

* `DATA`: A storage device type for the data in this column group. Acceptable values: ```ssd```, ```rot```.
* `COMPRESSION`: A data compression codec. Acceptable values: ```off```, ```lz4```.

By default, all columns are in the same group named ```default```.  If necessary, the parameters of this group can also be redefined.

In the example below, for the created table, the ```family_large``` group of columns is added and set for the ```series_info``` column, and the parameters for the default group, which is set by ```default``` for all other columns, are also redefined.

```sql
CREATE TABLE series_with_families (
    series_id Uint64,
    title Utf8,
    series_info Utf8 FAMILY family_large,
    release_date Uint64,
    PRIMARY KEY (series_id),
    FAMILY default (
        DATA = "ssd",
        COMPRESSION = "off"
    ),
    FAMILY family_large (
        DATA = "rot",
        COMPRESSION = "lz4"
    )
);
```

{% note info %}

Available types of storage devices depend on the {{ ydb-short-name }} cluster configuration.

{% endnote %}

{% endif %}

{% endif %}

{% if feature_olap_tables %}

## Сolumn-oriented tables {#olap-tables}

{% note warning %}

Column-oriented {{ ydb-short-name }} tables are in the Preview mode.

{% endnote %}

The `CREATE TABLE` statement creates a [column-oriented](../../../../concepts/datamodel/table.md#olap-data-types) table with the specified data schema and key columns (`PRIMARY KEY`).

```sql
CREATE TABLE table_name (
    column1 type1,
    column2 type2 NOT NULL,
    column2 type2,
    ...
    columnN typeN,
    PRIMARY KEY ( column, ... ),
    ...
)
PARTITION BY HASH(column1, column2, ...)
WITH (
    STORE = COLUMN,
    key = value,
    ...
)
```

### Columns {#olap-columns}

Data types supported by column-oriented tables and constraints imposed on data types in primary keys or data columns are described in the [supported data types](../../../../concepts/datamodel/table.md#olap-data-types) section for column-oriented tables.

Make sure to add the `PRIMARY KEY` and `PARTITION BY` clauses with a non-empty list of columns.

If you omit modifiers, a column is assigned an [optional](../../types/optional.md) type and can accept `NULL` values. To create a non-optional type, use `NOT NULL`.

**Example**

```sql
CREATE TABLE my_table (
    a Uint64 NOT NULL,
    b String,
    c Float,
    PRIMARY KEY (b, a)
)
PARTITION BY HASH(b)
WITH (
STORE = COLUMN
)
```

### Additional parameters {#olap-additional}

You can also specify a number of {{ backend_name }}-specific parameters for the table. When you create a table, those parameters are listed in the ```WITH``` clause:

```sql
CREATE TABLE table_name (...)
WITH (
    key1 = value1,
    key2 = value2,
    ...
)
```

Here, `key` is the name of the parameter and `value` is its value.

Supported parameters in column-oriented tables:

* `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT` sets the minimum physical number of partitions used to store data (see [{#T}](../../../../concepts/datamodel/table.md#olap-tables-partitioning)).

For example, the following code creates a column-oriented table with ten partitions:

```sql
CREATE TABLE my_table (
    id Uint64,
    title Utf8,
    PRIMARY KEY (id)
)
PARTITION BY HASH(id)
WITH (
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
);
```

{% endif %}
