## COALESCE {#coalesce}

Iterates through the arguments from left to right and returns the first non-empty argument found. To be sure that the result is non-empty (not of an [optional type](../../../types/optional.md)), the rightmost argument must be of this type (often a literal is used for this). With a single argument, returns this argument unchanged.

Lets you pass potentially empty values to functions that can't handle them by themselves.

A short format using the low-priority `??` operator is available (lower than the Boolean operations). You can use the `NVL` alias.

**Examples**

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

<span style="color: gray;">(all three examples above are equivalent)</span>

