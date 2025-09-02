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

The properties and capabilities of columnar tables are described in the article [{#T}](../../../../concepts/datamodel/table.md), and the specifics of their creation through YQL are described on the page [{#T}](./index.md).

## Time to Live (TTL) {#time-to-live}

The TTL (Time to Live) — the lifespan of a row — can be specified in the WITH clause for row-based and columnar tables. [TTL](../../../../concepts/ttl.md) automatically deletes rows or evicts them to external storage when the specified number of seconds has passed since the time recorded in the TTL column. TTL can be specified when creating row-based and columnar tables or added later using the `ALTER TABLE` command only for row-based tables.

The short form of the TTL value for specifying the time to delete rows:

```yql
Interval("<literal>") ON column [AS <unit>]
```

The general form of the TTL value:

```yql
Interval("<literal1>") action1, ..., Interval("<literalN>") actionN ON column [AS <unit>]
```

* `action` — the action performed when the TTL expression triggers. Allowed values:
    * `DELETE` — delete the row;
    * `TO EXTERNAL DATA SOURCE <path>` — evict the row to external storage specified by the [external data source](../../../../concepts/datamodel/external_data_source.md) at the path `<path>`.
* `<unit>` — the unit of measurement, specified only for columns with a [numeric type](../../../../concepts/ttl.md#restrictions):
    * `SECONDS`;
    * `MILLISECONDS`;
    * `MICROSECONDS`;
    * `NANOSECONDS`.

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

Example of creating a column-oriented table with eviction to external storage:

{% include [OLTP_not_allow_note](../../../../_includes/not_allow_for_oltp_note.md) %}

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
    TTL =
        Interval("PT1D") TO EXTERNAL DATA SOURCE `/Root/s3`,
        Interval("P2D") DELETE
    ON b
);
```

{% endif %}
