## IS \[NOT\] NULL {#is-null}

Matching an empty value (`NULL`). Since `NULL` is a special value [equal to nothing](../../../types/optional.md#null_expr), the ordinary [comparison operators](#comparison-operators) can't be used to match it.

**Examples**

```yql
SELECT key FROM my_table
WHERE value IS NOT NULL;
```

