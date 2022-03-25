## FROM ... SELECT ... {#from-select}

An inverted format, first specifying the data source and then the operation.

**Examples**

```yql
FROM my_table SELECT key, value;
```

```yql
FROM a_table AS a
JOIN b_table AS b
USING (key)
SELECT *;
```

