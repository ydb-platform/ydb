# Additional parameters (WITH)

You can also specify a number of {{ backend_name }}-specific parameters for the table. When you create a table, those parameters are listed in the ```WITH``` clause:

```yql
CREATE TABLE table_name (...)
WITH (
    key1 = value1,
    key2 = value2,
    ...
)
```

Here, `key` is the name of the parameter and `value` is its value.

The list of allowable parameter names and their values is provided on the table description page [{{ backend_name }}]({{ concept_table }}).

For example, such a query will create a string table with automatic partitioning enabled based on partition size and a preferred size of each partition being 512 megabytes:

```yql
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

{% if backend_name == "YDB" %}


A colum-oriented table is created by specifying the parameter `STORE = COLUMN` in the `WITH` clause:

```sql
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

The properties and capabilities of columnar tables are described in the article [{#T}](../../../../concepts/datamodel/table.md), and the specifics of their creation through YQL are described on the page [{#T}](./index.md). Also, the TTL (Time to Live) — the lifespan of a row — can be specified in the WITH clause for row-based and columnar tables. [TTL](../../../../concepts/ttl.md) automatically deletes rows when the specified number of seconds has passed since the time recorded in the TTL column. TTL can be specified when creating row-based and columnar tables or added later using the `ALTER TABLE` command only for row-based tables.

Example of creating a row-oriented and column-oriented tables with TTL:

{% list tabs %}

- Creating row-oriented table with TTL

    ```sql
    CREATE TABLE my_table (
        id Uint64,
        title Utf8,
        expire_at Timestamp,
        PRIMARY KEY (id)
    )
    WITH (
        TTL = Interval("PT0S") ON expire_at
    );
    ```

- Creating column-oriented table with TTL

    ```sql
    CREATE TABLE table_name (
        a Uint64 NOT NULL,
        b Timestamp NOT NULL,
        c Float,
        PRIMARY KEY (a, b)
    )
    PARTITION BY HASH(b)
    WITH (
        STORE = COLUMN,
        TTL = Interval("PT0S") ON b
    );
    ```

{% endlist %}

{% endif %}