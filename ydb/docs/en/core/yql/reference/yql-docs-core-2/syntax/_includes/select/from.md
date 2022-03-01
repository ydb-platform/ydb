## FROM {#from}

Data source for `SELECT`. The argument can accept the table name, the result of another `SELECT`, or a [named expression](../../expressions.md#named-nodes). Between `SELECT` and `FROM`, list the comma-separated column names from the source (or `*` to select all columns).

{% if feature_mapreduce %}The table is searched by name in the database specified by the operator [USE](../../use.md).{% endif %}

**Examples**

```yql
SELECT key FROM my_table;
```

```yql
SELECT * FROM
  (SELECT value FROM my_table);
```

```yql
$table_name = "my_table";
SELECT * FROM $table_name;
```

