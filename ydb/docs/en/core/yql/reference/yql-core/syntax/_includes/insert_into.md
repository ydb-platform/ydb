# INSERT INTO

{% if select_command != "SELECT STREAM" %} Adds rows to the table. {% if feature_bulk_tables %} If the target table already exists and is not sorted, the operation `INSERT INTO` adds rows at the end of the table. In the case of a sorted table, YQL tries to preserve sorting by running a sorted merge. {% endif %}{% if feature_map_tables %} If you try to insert a row into a table with an existing primary key value, the operation fails with the `PRECONDITION_FAILED` error code and the `Operation aborted due to constraint violation: insert_pk` message returned.{% endif %}

{% if feature_mapreduce %}The table is searched by name in the database specified by the [USE](../use.md) operator.{% endif %}

`INSERT INTO` lets you perform the following operations:

* Adding constant values using [`VALUES`](../values.md).

  ```sql
  INSERT INTO my_table (Key1, Key2, Value1, Value2)
  VALUES (345987,'ydb', 'Pied piper', 1414);
  COMMIT;
  ```

  ```sql
  INSERT INTO my_table (key, value)
  VALUES ("foo", 1), ("bar", 2);
  ```

* Saving the `SELECT` result.

  ```sql
  INSERT INTO my_table
  SELECT Key AS Key1, "Empty" AS Key2, Value AS Value1
  FROM my_table1;
  ```

{% else %}

Send the result of the [SELECT STREAM](../select_stream.md) calculation to the specified stream on the cluster specified by the [USE](../use.md) operator. The stream must exist and have a scheme matching the query result.

**Examples:**

```yql
INSERT INTO my_stream_dst
SELECT STREAM key FROM my_stream_src;
```

You can specify a table on the ydb cluster as the target. The table must exist at the time you create the operation. The table schema must be compatible with the type of query result.

**Examples:**

```yql
INSERT INTO ydb_cluster.`my_table_dst`
SELECT STREAM * FROM rtmr_cluster.`my_stream_source`;
```

{% endif %}

