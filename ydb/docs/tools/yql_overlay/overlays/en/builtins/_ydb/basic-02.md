### Examples

```yql
SELECT COALESCE(
  maybe_empty_column,
  "it's empty!"
) FROM my_table;
```

```yql
SELECT
  maybe_empty_column ?? "it's empty!"
FROM my_table;
```

```yql
SELECT NVL(
  maybe_empty_column,
  "it's empty!"
) FROM my_table;
```

All three examples above are equivalent.
