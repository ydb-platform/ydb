# CREATE TABLE

{% if feature_bulk_tables %}

The table is automatically created upon the first [INSERT INTO](../insert_into.md){% if feature_mapreduce %} in the database specified by the [USE](../use.md) operator{% endif %}. The schema is defined automatically in this process.

{% else %}

The invocation of `CREATE TABLE` creates {% if concept_table %}a [table]({{ concept_table }}){% else %}a table{% endif %} with the specified data schema{% if feature_map_tables %} and primary key columns (`PRIMARY KEY`){% endif %}.{% if feature_secondary_index == true %} It also allows defining secondary indexes on the created table.

{% endif %}

{% endif %}

```yql
CREATE TABLE [IF NOT EXISTS] <table_name> (
  [<column_name> <column_data_type>] [FAMILY <family_name>] [NULL | NOT NULL] [DEFAULT <default_value>]
  [, ...],
    INDEX <index_name>
      [GLOBAL]
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

## Request parameters

### table_name

The path of the table to be created.

When choosing a name for the table, consider the common [schema object naming rules](../../../../concepts/datamodel/cluster-namespace.md#object-naming-rules).

### IF NOT EXISTS

If the table with the specified name already exists, the execution of the operator is completely skipped — no checks or schema matching is performed, and no error occurs. Note that the existing table may differ in structure from the one you would like to create with this query — no comparison or equivalence check is performed.

### column_name

The name of the column to be created in the new table.

When choosing a name for the column, consider the common [column naming rules](../../../../concepts/datamodel/table.md#column-naming-rules).

### column_data_type

The data type of the column. The complete list of data types supported by {{ ydb-short-name }} is available in the [{#T}](../../types/index.md) section.

{% include [column_option_list.md](../_includes/column_option_list.md) %}

### INDEX

Definition of an index on the table. [Secondary indexes](secondary_index.md) and [vector indexes](vector_index.md) are supported.

### PRIMARY KEY

Definition of the primary key of the table. Specifies the columns that make up the primary key in the order of enumeration. For more information on selecting a primary key, see the [{#T}](../../../../dev/primary-key/index.md) article.

### PARTITION BY HASH

Definition of the columns on which partitioning will occur for **column-oriented** tables. Specifies the columns on which [partitioning](../../../../concepts/glossary.md#partition) will occur using the hash function. The columns must be part of the primary key. The columns do not necessarily have to be a prefix or suffix — the requirement is to be part of the primary key.

If the parameter is not specified, the table will be partitioned on the same columns as those included in the primary key. For more information on selecting and working with partition keys in column-oriented tables, see the [{#T}](../../../../dev/primary-key/column-oriented.md) article.

For more information on partitioning column-oriented tables, see the [{#T}](../../../../concepts/datamodel/table.md#olap-tables-partitioning) section.

### FAMILY <column_family> (column group setting)

Definition of a column group with specified parameters. For more information, see the [{#T}](family.md) section.

### WITH

Additional parameters for creating a table. For more information, see the [{#T}](with.md) section.

{ % note info % }

{{ ydb-short-name }} supports two types of tables:

* [Row-oriented](../../../../concepts/datamodel/table.md#row-oriented-tables) tables.
* [Column-oriented](../../../../concepts/datamodel/table.md#column-oriented-tables) tables.

The table type is specified by the `STORE` parameter in the `WITH` clause, where `ROW` indicates a [row-oriented](../../../../concepts/datamodel/table.md#row-oriented-tables) table and `COLUMN` indicates a [column-oriented](../../../../concepts/datamodel/table.md#column-oriented-tables) table:

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

When choosing a name for the table, consider the common [schema object naming rules](../../../../concepts/datamodel/cluster-namespace.md#object-naming-rules).

{% endnote %}

### AS SELECT

Creating and filling a table with data from a `SELECT` query. For more information, see the [{#T}](as_select.md) section.

## Examples of table creation {#examples-tables-creation}

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

  Example of creating a table with a DEFAULT value:

  ```yql
  CREATE TABLE table_with_default (
    id Uint64,
    name String DEFAULT "unknown",
    score Double NOT NULL DEFAULT 0.0,
    PRIMARY KEY (id)
  );
  ```

  {% if feature_column_container_type == true %}

  For non-key columns, any data types are allowed, whereas for key columns only [primitive](../../types/primitive.md) types are permitted. When specifying complex types (for example, List<String>), the type should be enclosed in double quotes.

  {% else %}

  For both key and non-key columns, only [primitive](../../types/primitive.md) data types are allowed.

  {% endif %}

  {% if feature_not_null == true %}

  Without additional modifiers, a column acquires an [optional](../../types/optional.md) type and allows `NULL` values. To designate a non-optional type, use the `NOT NULL` constraint.

  {% else %}

  {% if feature_not_null_for_pk %}

  By default, all columns are [optional](../../types/optional.md) and can have `NULL` values. The `NOT NULL` constraint can only be specified for columns that are part of the primary key.

  {% else %}

  All columns allow NULL values, meaning they are [optional](../../types/optional.md).

  {% endif %}

  {% endif %}

  {% if feature_map_tables %}

  Specifying a `PRIMARY KEY` with a non-empty list of columns is mandatory. These columns become part of the key in the order they are listed.

  {% endif %}

  Example of creating a row-oriented table using partitioning options:

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

  Such code will create a row-oriented table with automatic partitioning by partition size (`AUTO_PARTITIONING_BY_SIZE`) enabled, and with the preferred size of each partition (`AUTO_PARTITIONING_PARTITION_SIZE_MB`) set to 512 megabytes. The full list of row-oriented table partitioning options can be found in the [Partitioning Row-Oriented Tables](../../../../concepts/datamodel/table.md#partitioning_row_table) section.


- Creating a column-oriented table

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

  For column-oriented tables, you can explicitly specify the columns on which partitioning will occur using the `PARTITION BY HASH` construct. Usually, these are columns of the primary key with a large number of unique values, such as `Timestamp`. If `PARTITION BY HASH` is not specified, partitioning will occur automatically on all columns included in the primary key. For more information on selecting and working with partition keys in column-oriented tables, see the [{#T}](../../../../dev/primary-key/column-oriented.md) article.

  It is important to specify the correct number of partitions when creating a column-oriented table with the `AUTO_PARTITIONING_MIN_PARTITIONS_COUNT` parameter:

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

  This code will create a columnar table with 10 partitions. The full list of column-oriented table partitioning options can be found in the [{#T}](../../../../concepts/datamodel/table.md#olap-tables-partitioning) section.

{% endlist %}

{% else %}

{% if feature_column_container_type == true %}

For non-key columns, any data types are allowed, whereas for key columns only [primitive](../../types/primitive.md) types are permitted. When specifying complex types (for example, `List<String>`), the type should be enclosed in double quotes.

{% else %}

For both key and non-key columns, only [primitive](../../types/primitive.md) data types are allowed.

{% endif %}

{% if feature_not_null == true %}

Without additional modifiers, a column acquires an [optional](../../types/optional.md) type and allows `NULL` values. To designate a non-optional type, use the `NOT NULL` constraint.

{% else %}

{% if feature_not_null_for_pk %}

By default, all columns are [optional](../../types/optional.md) and can have `NULL` values. The `NOT NULL` constraint can only be specified for columns that are part of the primary key.

{% else %}

All columns allow NULL values, meaning they are [optional](../../types/optional.md).

{% endif %}

{% endif %}

{% if feature_map_tables %}

Specifying a `PRIMARY KEY` with a non-empty list of columns is mandatory. These columns become part of the key in the order they are listed.

{% endif %}

### Example

```yql
CREATE TABLE <table_name> (
  a Uint64,
  b Uint64,
  c Float,
  PRIMARY KEY (a, b)
);
```

{% endif %}

{% if backend_name == "YDB" %}

When creating row-oriented tables, it is possible to specify:

* [A secondary index](secondary_index.md).
* [A vector index](vector_index.md).
* [Column groups](family.md).
* [Additional parameters](with.md).
* [Creating a table filled with query results](as_select.md).

When creating column-oriented tables, it is possible to specify:

* [Column groups](family.md).
* [Additional parameters](with.md).
* [Creating a table filled with query results](as_select.md).

{% endif %}
