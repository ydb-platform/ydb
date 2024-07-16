# Explicitly created temporary (anonymous) tables {#temporary-tables}

In complex multiphase queries, it can be useful to explicitly create a physical temporary table in order to manually affect the query process. To do this, you can use the table name starting with `@`. Such tables are called anonymous to distinguish them from temporary tables created by a YT operation.

Each such name within the query is replaced, at execution time, by a globally unique path to a table in a temporary directory. Such temporary tables are automatically deleted upon completion of the query, following the same rules as implicitly created tables.

This feature lets you ignore conflicts in paths to temporary tables between parallel operations, and also avoid deleting them explicitly when the query completes.

**Examples:**

```yql
INSERT INTO @my_temp_table
SELECT * FROM my_input_table ORDER BY value;

COMMIT;

SELECT * FROM @my_temp_table WHERE value = "123"
UNION ALL
SELECT * FROM @my_temp_table WHERE value = "456";
```

Temporary table names can use [named expressions](../../expressions.md#named-nodes):

```yql
$tmp_name = "my_temp_table";

INSERT INTO @$tmp_name
SELECT 1 AS one, 2 AS two, 3 AS three;

COMMIT;

SELECT * FROM @$tmp_name;
```

