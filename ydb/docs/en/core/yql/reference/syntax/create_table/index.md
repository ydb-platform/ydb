# CREATE TABLE

{% if feature_bulk_tables %}

The table is created automatically on the first [INSERT INTO](../insert_into.md){% if feature_mapreduce %}, in the database specified by the [USE](../use.md) operator{% endif %}. The schema is determined automatically.

{% else %}

Calling `CREATE TABLE` creates {% if concept_table %} [table]({{ concept_table }}){% else %}table{% endif %} with the specified data schema{% if feature_map_tables %} and key columns (`PRIMARY KEY`){% endif %}.{% if feature_secondary_index == true %} Allows defining secondary indexes on the created table.

{% endif %}

{% endif %}


```yql
CREATE TABLE [IF NOT EXISTS] <table_name> (
  [<column_name> <column_data_type>] [FAMILY <family_name>] [NULL | NOT NULL] [DEFAULT <default_value>]
  [COMPRESSION([algorithm=<algorithm_name>[, level=<value>]])]
  [ENCODING([OFF|DICT])]
  [, ...],
    INDEX <index_name>
      [GLOBAL|LOCAL]
      [UNIQUE]
      [SYNC|ASYNC]
      [USING <index_type>]
      ON ( <index_columns> )
      [COVER ( <cover_columns> )]
      [WITH ( <parameter_name> = <parameter_value>[, ...])]
    [, ...]
  PRIMARY KEY ( <column>[, ...]),
  [FAMILY <column_family> ( family_options[, ...])]
)
[PARTITION BY HASH ( <column>[, ...])]
[WITH (<setting_name> = <setting_value>[, ...])]

[AS SELECT ...]
```


{% if oss == true and backend_name == "YDB" %}

## Query parameters

### table_name

Path of the table to be created.

When choosing a name for the table, consider the common [schema object naming rules](../../../../concepts/datamodel/cluster-namespace.md#object-naming-rules).

### IF NOT EXISTS

If a table with the specified name already exists, the statement execution is completely skipped — no schema checks or matching are performed, and no error is raised. Note that the existing table may differ in structure from the one you intended to create with this query — no comparison or equivalence verification is performed.

### column_name

Name of the column created in the new table.

When choosing a name for a column, consider the common [column naming rules](../../../../concepts/datamodel/table.md#column-naming-rules).

### column_data_type

Data type of the column. The full list of data types supported by {{ ydb-short-name }} is available in the [{#T}](../../types/index.md) section.

{% include [column_option_list.md](../_includes/column_option_list.md) %}

### INDEX

Definition of an index on a table. Supported:

* [secondary indexes](secondary_index.md),
* [vector indexes](vector_index.md),
* [full-text indexes](fulltext_index.md),
* [Bloom indexes](bloom_skip_index.md),
* [JSON indexes](json_index.md).

### PRIMARY KEY

Definition of the table's primary key. Specifies the columns that make up the primary key in the order listed. See more about choosing a primary key in the [{#T}](../../../../dev/primary-key/index.md) section.

### PARTITION BY HASH

Definition of partitioning keys for **columnar** tables. Specifies the columns whose hash is used for data [partitioning](../../../../concepts/glossary.md#partition). Columns must be part of the primary key. The columns do not have to be a prefix or suffix — the requirement is to be part of the primary key.

If the parameter is not specified, the table will be partitioned by the same columns that are part of the primary key. For guidance on choosing partitioning keys in columnar tables, read the article [{#T}](../../../../dev/primary-key/column-oriented.md).

Read more about partitioning columnar tables in the [{#T}](../../../../concepts/datamodel/table.md#olap-tables-partitioning) section.

### FAMILY <column_family> (column group configuration)

Definition of a column group with specified parameters. See more in the [{#T}](family.md) section.

### WITH

Additional table creation parameters. See more in the [{#T}](with.md) section.

{% note info %}

{{ ydb-short-name }} supports two types of tables:

* [Row](../../../../concepts/datamodel/table.md#row-oriented-tables).
* [Columnar](../../../../concepts/datamodel/table.md#column-oriented-tables).

The table type at creation is set by the `STORE` parameter in the `WITH` block, where `ROW` denotes a [row table](../../../../concepts/datamodel/table.md#row-oriented-tables), and `COLUMN` denotes a [columnar table](../../../../concepts/datamodel/table.md#column-oriented-tables):


```yql
CREATE <table_name> (
  columns
  ...
)

WITH (
  STORE = COLUMN -- Default value ROW
)
```


By default, if the `STORE` parameter is not specified, a row table is created.

{% endnote %}

{% note info %}

When choosing a name for the table, consider the common [schema object naming rules](../../../../concepts/datamodel/cluster-namespace.md#object-naming-rules).

{% endnote %}

### AS SELECT

Creating and populating a table based on the results of the `SELECT` query. See more in the [{#T}](as_select.md) section.

## Table creation examples

{% list tabs %}

- Creating a row table

  {% if feature_column_container_type %}

  ```yql
    CREATE TABLE <table_name> (
      a Uint64,
      b Uint64,
      c Float,
      d "List<List<Int32>>"
      PRIMARY KEY (a, b)
    );
  ```

  {% else %}

  ```yql
    CREATE TABLE <table_name> (
      a Uint64,
      b Uint64,
      c Float,
      PRIMARY KEY (a, b)
    );
  ```

  {% endif %}

  Example of creating a table using the default value (DEFAULT):


  ```yql
    CREATE TABLE table_with_default (
    id Uint64,
    name String DEFAULT "unknown",
    score Double NOT NULL DEFAULT 0.0,
    PRIMARY KEY (id)
  );
  ```


  {% if feature_column_container_type == true %}

  For non-key columns, any data types are allowed{% if feature_serial %} , except [serial](../../types/serial.md) {% endif %}, for key columns only [primitive](../../types/primitive.md){% if feature_serial %} and [serial](../../types/serial.md){% endif %}. When specifying complex types (e.g., `List<String>`), the type is enclosed in double quotes.

  {% else %}

  {% if feature_serial %}

  For key columns, only [primitive](../../types/primitive.md) and [serial](../../types/serial.md) data types are allowed, for non-key columns, only [primitive](../../types/primitive.md) types are allowed.

  {% else %}

  For both key and non-key columns, only [primitive](../../types/primitive.md) data types are allowed.

  {% endif %}

  {% endif %}

  {% if feature_not_null == true %}

  Without additional modifiers, the column gets an [optional type](../../types/optional.md) and allows writing `NULL` as values. To obtain a non-optional type, you must use `NOT NULL`.

  {% else %}

  {% if feature_not_null_for_pk %}

  By default, all columns are [optional](../../types/optional.md) and can have a NULL value. The `NOT NULL` constraint can be specified only for columns that are part of the primary key.

  {% else %}

  All columns allow writing `NULL` as values, i.e., they are [optional](../../types/optional.md).

  {% endif %}

  {% endif %}

  {% if feature_map_tables %}

  Specifying `PRIMARY KEY` with a non‑empty list of columns is required. These columns become part of the key in the order listed.

  {% endif %}

  Example of creating a row table with partitioning options:


  ```yql
  CREATE TABLE <table_name> (
    a Uint64,
    b Uint64,
    c Float,
    PRIMARY KEY (a, b)
  )
  WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED,
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 512
  );
  ```


  This code will create a row table with automatic partitioning enabled by partition size (`AUTO_PARTITIONING_BY_SIZE`) and a preferred size of each partition (`AUTO_PARTITIONING_PARTITION_SIZE_MB`) of 512 megabytes. The full list of row‑table partitioning options is in the [Row table partitioning](../../../../concepts/datamodel/table.md#partitioning_row_table) section of the article [{#T}](../../../../concepts/datamodel/table.md).

- Creating a column table

  ```yql
  CREATE TABLE table_name (
    a Uint64 NOT NULL,
    b Timestamp NOT NULL,
    c Float,
    PRIMARY KEY (a, b)
  )
  PARTITION BY HASH(b)
  WITH (
    STORE = COLUMN
  );
  ```


  For column tables you can explicitly specify which columns to partition by using the `PARTITION BY HASH` construct. Typically, you choose primary‑key columns with a high number of unique values, for example, `Timestamp`. If `PARTITION BY HASH` is omitted, partitioning will occur automatically on all columns that are part of the primary key. For more details on selecting and using partitioning keys in column tables, read the article [{#T}](../../../../dev/primary-key/column-oriented.md).

  Currently, column tables do not support automatic re‑partitioning, so it is important to specify the correct number of partitions when creating a table using the `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT` parameter:


  ```yql
  CREATE TABLE table_name (
    a Uint64 NOT NULL,
    b Timestamp NOT NULL,
    c Float,
    PRIMARY KEY (a, b)
  )
  PARTITION BY HASH(b)
  WITH (
    STORE = COLUMN,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
  );
  ```


  This code will create a column table with 10 partitions. You can review the full list of column‑table partitioning options in the [{#T}](../../../../concepts/datamodel/table.md#olap-tables-partitioning) section of the article [{#T}](../../../../concepts/datamodel/table.md).

{% endlist %}

{% else %}

{% if feature_column_container_type == true %}

For non‑key columns any data types are allowed, for key columns only [primitive](../../types/primitive.md) types. When specifying complex types (for example, `List<String>`) the type must be enclosed in double quotes.

{% else %}

Only [primitive](../../types/primitive.md) data types are allowed for both key and non‑key columns.

{% endif %}

{% if feature_not_null == true %}

Without additional modifiers, a column has an [optional type](../../types/optional.md) and allows writing `NULL` as values. To obtain a non‑optional type you must use `NOT NULL`.

{% else %}

{% if feature_not_null_for_pk %}

By default all columns are [optional](../../types/optional.md) and can have a NULL value. The `NOT NULL` constraint can be specified only for columns that are part of the primary key.

{% else %}

All columns allow writing `NULL` as values, i.e., they are [optional](../../types/optional.md).

{% endif %}

{% endif %}

{% if feature_map_tables %}

Specifying `PRIMARY KEY` with a non‑empty list of columns is required. These columns become part of the key in the order listed.

{% endif %}

Example:


```yql
CREATE TABLE <table_name> (
  a Uint64,
  b Uint64,
  c Float,
  PRIMARY KEY (a, b)
);
```

{% endif %}

{% if backend_name == "YDB" and oss == true %}

When creating row tables you can specify:

* [Secondary index](secondary_index.md).
* [Vector index](vector_index.md).
* [Full‑text index](fulltext_index.md).
* [JSON index](json_index.md).
* [Bloom index](bloom_skip_index.md).
* [Column groups](family.md).
* [Additional parameters](with.md).
* [Creating and populating a table from query results](as_select.md).

When creating column tables you can specify:

* [Bloom index](bloom_skip_index.md).
* [Column groups](family.md).
* [Additional parameters](with.md).
* [Creating and populating a table from query results](as_select.md).

{% endif %}
