# CREATE TABLE

{% if feature_bulk_tables %}

The table is created automatically on the first [INSERT INTO](../insert_into.md){% if feature_mapreduce %}, in the database specified by the [USE](../use.md) statement{% endif %}. The schema is determined automatically.

{% else %}

The `CREATE TABLE` call creates {% if concept_table %} [a table]({{ concept_table }}){% else %}a table{% endif %} with the specified data schema{% if feature_map_tables %} and key columns (`PRIMARY KEY`){% endif %}.{% if feature_secondary_index == true %} It allows defining secondary indexes on the created table.

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

Path of the created table.

When choosing a table name, follow the general [rules for naming schema objects](../../../../concepts/datamodel/cluster-namespace.md#object-naming-rules).

### IF NOT EXISTS

If a table with the specified name already exists, the statement execution is completely skipped — no checks or schema matching are performed, and no error occurs. Note that the existing table may differ in structure from the one you intended to create with this query — no comparison or equivalence check is performed.

### column_name

Name of the column being created in the new table.

When choosing a column name, follow the general [rules for naming columns](../../../../concepts/datamodel/table.md#column-naming-rules).

### column_data_type

Column data type. The full list of data types supported by {{ ydb-short-name }} is available in the [{#T}](../../types/index.md) section.

{% include [column_option_list.md](../_includes/column_option_list.md) %}

### INDEX

Defining an index on a table. Supported:

* [secondary indexes](secondary_index.md),
* [vector indexes](vector_index.md),
* [full-text indexes](fulltext_index.md),
* [Bloom indexes](bloom_skip_index.md),
* [JSON indexes](json_index.md).

### PRIMARY KEY

Defining the table's primary key. Specifies the columns that make up the primary key in the order listed. For more details on choosing a primary key, see the [{#T}](../../../../dev/primary-key/index.md) section.

### PARTITION BY HASH

Defining partitioning keys for **columnar** tables. Specifies the columns by whose hash [partitioning](../../../../concepts/glossary.md#partition) of data is performed. The columns must be part of the primary key. However, the columns do not necessarily have to be a prefix or suffix -- the requirement is to be part of the primary key.

If the parameter is not specified, the table will be partitioned by the same columns that are part of the primary key. For guidance on how to choose partitioning keys for columnar tables, see the article [{#T}](../../../../dev/primary-key/column-oriented.md).

For more details on partitioning columnar tables, see the [{#T}](../../../../concepts/datamodel/table.md#olap-tables-partitioning) section.

### FAMILY <column_family> (column family settings)

Defining a column family with specified parameters. For more details, see the [{#T}](family.md) section.

### WITH

Additional table creation parameters. For more details, see the [{#T}](with.md) section.

{% note info %}

{{ ydb-short-name }} supports two types of tables:

* [Row-oriented](../../../../concepts/datamodel/table.md#row-oriented-tables).
* [Columnar](../../../../concepts/datamodel/table.md#column-oriented-tables).

The table type at creation is set by the `STORE` parameter in the `WITH` block, where `ROW` means [row-oriented table](../../../../concepts/datamodel/table.md#row-oriented-tables) and `COLUMN` means [columnar table](../../../../concepts/datamodel/table.md#column-oriented-tables):


```yql
CREATE <table_name> (
  columns
  ...
)

WITH (
  STORE = COLUMN -- Default value ROW
)
```


By default, if the `STORE` parameter is not specified, a row-oriented table is created.

{% endnote %}

{% note info %}

When choosing a table name, follow the general [rules for naming schema objects](../../../../concepts/datamodel/cluster-namespace.md#object-naming-rules).

{% endnote %}

### AS SELECT

Creating and populating a table based on the results of the `SELECT` query. For more details, see the [{#T}](as_select.md) section.

## Examples of creating tables

{% list tabs %}

- Creating a row-oriented table

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

  Example of creating a table using a default value (DEFAULT):


  ```yql
    CREATE TABLE table_with_default (
    id Uint64,
    name String DEFAULT "unknown",
    score Double NOT NULL DEFAULT 0.0,
    PRIMARY KEY (id)
  );
  ```


  {% if feature_column_container_type == true %}

  For non-key columns, any data types are allowed{% if feature_serial %}, except [serial](../../types/serial.md) {% endif %}; for key columns, only [primitive](../../types/primitive.md){% if feature_serial %} and [serial](../../types/serial.md){% endif %} types are allowed. When specifying complex types (e.g., `List<String>`), the type is enclosed in double quotes.

  {% else %}

  {% if feature_serial %}

  For key columns, only [primitive](../../types/primitive.md) and [serial](../../types/serial.md) data types are allowed; for non-key columns, only [primitive](../../types/primitive.md) data types are allowed.

  {% else %}

  For key and non-key columns, only [primitive](../../types/primitive.md) data types are allowed.

  {% endif %}

  {% endif %}

  {% if feature_not_null == true %}

  Without additional modifiers, the column acquires an [optional type](../../types/optional.md) and allows `NULL` as values. To obtain a non-optional type, use `NOT NULL`.

  {% else %}

  {% if feature_not_null_for_pk %}

  By default, all columns are [optional](../../types/optional.md) and can have NULL values. The `NOT NULL` constraint can only be specified for columns that are part of the primary key.

  {% else %}

  All columns allow `NULL` as a value, meaning they are [optional](../../types/optional.md).

  {% endif %}

  {% endif %}

  {% if feature_map_tables %}

  It is mandatory to specify `PRIMARY KEY` with a non-empty list of columns. These columns become part of the key in the order they are listed.

  {% endif %}

  Example of creating a row table using partitioning options:


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


  This code will create a row table with automatic partitioning by partition size (`AUTO_PARTITIONING_BY_SIZE`) enabled and a preferred size of each partition (`AUTO_PARTITIONING_PARTITION_SIZE_MB`) of 512 megabytes. The full list of row table partitioning options is available in the [Row table partitioning](../../../../concepts/datamodel/table.md#partitioning_row_table) section of the [{#T}](../../../../concepts/datamodel/table.md) article.

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


  For column tables, you can explicitly specify which columns will be used for partitioning using the `PARTITION BY HASH` construct. Typically, primary key columns with a large number of unique values are chosen for this, for example, `Timestamp`. If `PARTITION BY HASH` is not specified, partitioning will occur automatically across all columns that are part of the primary key. For more details on selecting and working with partitioning keys in column tables, see the [{#T}](../../../../dev/primary-key/column-oriented.md) article.

  Currently, column tables do not support automatic repartitioning, so it is important to specify the correct number of partitions when creating a table using the `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT` parameter:


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


  This code will create a column table with 10 partitions. The full list of column table partitioning options can be found in the [{#T}](../../../../concepts/datamodel/table.md#olap-tables-partitioning) section of the [{#T}](../../../../concepts/datamodel/table.md) article.

{% endlist %}

{% else %}

{% if feature_column_container_type == true %}

For non-key columns, any data types are allowed; for key columns, only [primitive](../../types/primitive.md) types are allowed. When specifying complex types (for example, `List<String>`), the type is enclosed in double quotes.

{% else %}

For both key and non-key columns, only [primitive](../../types/primitive.md) data types are allowed.

{% endif %}

{% if feature_not_null == true %}

Without additional modifiers, a column acquires an [optional type](../../types/optional.md) and allows `NULL` as a value. To obtain a non-optional type, you must use `NOT NULL`.

{% else %}

{% if feature_not_null_for_pk %}

By default, all columns are [optional](../../types/optional.md) and can have a NULL value. The `NOT NULL` constraint can only be specified for columns that are part of the primary key.

{% else %}

All columns allow `NULL` as a value, meaning they are [optional](../../types/optional.md).

{% endif %}

{% endif %}

{% if feature_map_tables %}

It is mandatory to specify `PRIMARY KEY` with a non-empty list of columns. These columns become part of the key in the order they are listed.

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

When creating row tables, you can specify:

* [Secondary index](secondary_index.md).
* [Vector index](vector_index.md).
* [Full-text index](fulltext_index.md).
* [JSON index](json_index.md).
* [Bloom index](bloom_skip_index.md).
* [Column groups](family.md).
* [Additional parameters](with.md).
* [Creating and populating a table based on query results](as_select.md).

For column tables, when creating them, you can specify:

* [Bloom index](bloom_skip_index.md).
* [Column groups](family.md).
* [Additional parameters](with.md).
* [Creating and populating a table based on query results](as_select.md).

{% endif %}
