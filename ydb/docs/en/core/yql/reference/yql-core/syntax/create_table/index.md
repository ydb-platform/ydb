# CREATE TABLE

## CREATE TABLE syntax

{% if feature_bulk_tables %}

The table is automatically created upon the first [INSERT INTO](../insert_into.md){% if feature_mapreduce %} in the database specified by the [USE](../use.md) operator{% endif %}. The schema is defined automatically in this process.

{% else %}

The invocation of `CREATE TABLE` creates {% if concept_table %}a [table]({{ concept_table }}){% else %}a table{% endif %} with the specified data schema{% if feature_map_tables %} and primary key columns (`PRIMARY KEY`){% endif %}. {% if feature_secondary_index == true %}It also allows defining secondary indexes on the created table.

{% endif %}
{% endif %}

    CREATE [TEMP | TEMPORARY] TABLE table_name (
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

{% if oss == true and backend_name == "YDB" %}

{% if feature_olap_tables %}

{{ ydb-short-name }} supports two types of tables:

* [Row-oriented](../../../../concepts/datamodel/table.md#row-oriented-tables) tables.
* [Column-oriented](../../../../concepts/datamodel/table.md#column-oriented-tables) tables.

The table type is specified by the `STORE` parameter in the `WITH` clause, where `ROW` indicates a [row-oriented](../../../../concepts/datamodel/table.md#row-oriented-tables) table and `COLUMN` indicates a [column-oriented](../../../../concepts/datamodel/table.md#column-oriented-tables) table:
```sql
CREATE <table_name> (
  columns 
  ...
)

WITH (
  STORE = COLUMN -- Default value ROW
)
```

By default, if the `STORE` parameter is not specified, a row-oriented table is created.

{% endif %}

### Examples of table creation {#examples-tables-creation}

{% list tabs %}

- Creating a row-oriented table

  {% if feature_column_container_type %}
    ```sql
    CREATE TABLE <table_name> (
      a Uint64,
      b Uint64,
      c Float,
      d "List<List<Int32>>" 
      PRIMARY KEY (a, b)
    );
    ```
  {% else %}
    ```sql
    CREATE TABLE <table_name> (
      a Uint64,
      b Uint64,
      c Float,
      PRIMARY KEY (a, b)
    );
    ```
  {% endif %}

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

- Creating a column-oriented table

  ```sql
    CREATE TABLE table_name (
      a Uint64 NOT NULL,
      b Uint64 NOT NULL,
      c Float,
      PRIMARY KEY (a, b)
    )
    WITH (
      STORE = COLUMN
    );
    ```  

{% endlist %}

{% else %}

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

**Example**:
```sql
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
* [Column groups](family.md).
* [Additional parameters](with.md).

For column-oriented tables, only [additional parameters](with.md) can be specified during creation.

{% endif %}
