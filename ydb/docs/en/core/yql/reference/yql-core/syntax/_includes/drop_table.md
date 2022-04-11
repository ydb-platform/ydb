# DROP TABLE

Deletes the specified table.{% if feature_mapreduce %}  The table is searched by name in the database specified by the [USE](../use.md) operator.{% endif %}

If there is no such table, an error is returned.

**Examples:**

```yql
DROP TABLE my_table;
```

