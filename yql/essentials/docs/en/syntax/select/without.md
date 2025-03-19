# WITHOUT

Excluding columns from the result of `SELECT *`. The `IF EXISTS` modifier does not throw an error for missing columns.

## Examples

```yql
SELECT * WITHOUT foo, bar FROM my_table;
SELECT * WITHOUT IF EXISTS foo, bar FROM my_table;
```

```yql
PRAGMA simplecolumns;
SELECT * WITHOUT t.foo FROM my_table AS t
CROSS JOIN (SELECT 1 AS foo) AS v;
```

