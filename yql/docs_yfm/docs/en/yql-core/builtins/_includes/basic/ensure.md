## Ensure... {#ensure}

Checking for the user conditions:

* `Ensure()`: Checking whether the predicate is true at query execution.
* `EnsureType()`: Checking that the expression type exactly matches the specified type.
* `EnsureConvertibleTo()`: A soft check of the expression type (with the same rules as for implicit type conversion).

If the check fails, the entire query fails.

Arguments:

1. An expression that will result from a function call if the check is successful. It's also checked for the data type in the corresponding functions.
2. Ensure uses a Boolean predicate that is checked for being `true`. The other functions use the data type that can be obtained using the [relevant functions](../../types.md), or a string literal with a [text description of the type](../../../types/type_string.md).
3. An optional string with an error comment to be included in the overall error message when the query is complete. The data itself can't be used for type checks, since the data check is performed at query validation (or can be an arbitrary expression in the case of Ensure).

To check the conditions based on the final calculation result, it's convenient to combine Ensure with [DISCARD SELECT](../../../syntax/discard.md).

**Examples**

```yql
SELECT Ensure(
    value,
    value < 100,
    "value out or range"
) AS value FROM my_table;
```

```yql
SELECT EnsureType(
    value,
    TypeOf(other_value),
    "expected value and other_value to be of same type"
) AS value FROM my_table;
```

```yql
SELECT EnsureConvertibleTo(
    value,
    Double?,
    "expected value to be numeric"
) AS value FROM my_table;
```

