## Data types accepting NULL

Any typed data in YQL, including table columns, can be either non-nullable (guaranteed value) or nullable (empty value denoted as `NULL`). Data types that can include `NULL` values are called _optional_ or, in SQL terms, _nullable_.

Optional data types in the [text format](../type_string.md) use the question mark at the end (for example, `String?`) or the notation `Optional<...>`.
The following operations are most often performed on optional data types:

* [IS NULL](../../syntax/expressions.md#is-null): Matching an empty value
* [COALESCE](../../builtins/basic.md#coalesce): Leaves the filled values unchanged and replaces `NULL` with the default value that follows
* [UNWRAP](../../builtins/basic.md#optional-ops): Extract the value of the source type from the optional data type, `T?` is converted to `T`
* [JUST](../../builtins/basic#optional-ops) Change the data type to the optional type of the current one, `T` converts to`T?`
* [NOTHING](../../builtins/basic.md#optional-ops): Create an empty value with the specified type.

`Optional` (nullable) isn't a property of a data type or column, but a [container](../containers.md) type where containers can be arbitrarily nested into each other. For example, a column with the type `Optional<Optional<Boolean>>` can accept 4 values: `NULL` of the overarching container, `NULL` of the inner container, `TRUE`, and `FALSE`. The above-declared type differs from `List<List<Boolean>>`, because it uses `NULL` as an empty list, and you can't put more than one non-null element in it. You can also use `Optional<Optional<T>>` as a key [lookup](/docs/s_expressions/functions#lookup) in the dictionary (`Dict(k,v)`) with `Optional<T>` values. Using this type of result data, you can distinguish between a `NULL` value in the dictionary and a missing key.

**Example**

```sql
$dict = {"a":1, "b":null};
$found = $dict["b"];
select if($found is not null, unwrap($found), -1);
```

Result:

```text
# column0
0 null
```

## Logical and arithmetic operations with NULL {#null_expr}

The `NULL` literal has a separate singular `Null` type and can be implicitly converted to any optional type (for example, the nested type `OptionalOptional<T>...>>`). In ANSI SQL, `NULL` means "an unknown value", that's why logical and arithmetic operations involving `NULL` or empty `Optional` have certain specifics.

**Examples**

```
SELECT
    True OR NULL,        -- Just(True) (works the same way as True OR <unknown value of type Bool>)
    False AND NULL,      -- Just(False)
    True AND NULL,       -- NULL   (to be more precise, Nothing<Bool?> – <unknown value of type Bool>)
    NULL OR NOT NULL,    -- NULL (all NULLs are considered "different")
    1 + NULL,            -- NULL   (Nothing<Int32?>) - the result of adding 1 together with
                         -- an unknown Int value)
    1 == NULL,           -- NULL (the result of comparing 1 with an unknown Int value)
    (1, NULL) == (1, 2), -- NULL (composite elements are compared by component comparison
                         -- using `AND')
    (2, NULL) == (1, 3), -- Just(False) (the expression is equivalent to 2 == 1 AND NULL == 3)
```

