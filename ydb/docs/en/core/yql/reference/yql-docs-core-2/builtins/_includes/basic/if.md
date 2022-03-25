## IF {#if}

Checks the condition: `IF(condition_expression, then_expression, else_expression)`.

It's a simplified alternative for [CASE WHEN ... THEN ... ELSE ... END](../../../syntax/expressions.md#case).

You may omit the `else_expression` argument. In this case, if the condition is false (`condition_expression` returned `false`), an empty value is returned with the type corresponding to `then_expression` and allowing for `NULL`. Hence, the result will have an [optional data type](../../../types/optional.md).

**Examples**

```yql
SELECT
  IF(foo > 0, bar, baz) AS bar_or_baz,
  IF(foo > 0, foo) AS only_positive_foo
FROM my_table;
```

