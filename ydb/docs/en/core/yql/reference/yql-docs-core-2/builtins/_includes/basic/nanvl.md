## NANVL {#nanvl}

Replaces the values of `NaN` (not a number) in expressions like `Float`, `Double`, or [Optional](../../../types/optional.md).

Arguments:

1. The expression where you want to make a replacement.
2. The value to replace `NaN`.

If one of the arguments is `Double`, the result is`Double`, otherwise, it's `Float`. If one of the arguments is `Optional`, then the result is `Optional`.

**Examples**

```yql
SELECT
  NANVL(double_column, 0.0)
FROM my_table;
```

