# Additional parameters (WITH)

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

The list of allowable parameter names and their values is provided on the table description page {{ backend_name }}({{ concept_table }}).

For example, such a query will create a string table with automatic partitioning enabled based on partition size and a preferred size of each partition being 512 megabytes:
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

 {% if backend_name == "YDB" %}

In the `WITH` clause, you can also specify TTL (Time to Live) — the lifespan of a row. [TTL](../../../../concepts/ttl.md) automatically removes rows from the string table when the specified number of seconds have passed from the time recorded in the TTL column. TTL can be set when the table is created or added later via `ALTER TABLE`. The code below will create a string table with TTL:
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

 {% endif %}