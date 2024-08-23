## Data types accepting NULL

Any typed data in YQL, including table columns, can be either non-nullable (guaranteed value) or nullable (empty value denoted as `NULL`). Data types that can include `NULL` values are called _optional_ or, in SQL terms, _nullable_.

Optional data types in the [text format](../type_string.md) use the question mark at the end (for example, `String?`) or the notation `Optional<...>`.
The following operations are most often performed on optional data types:

* [IS NULL](../../syntax/expressions.md#is-null): Matching an empty value
* [COALESCE](../../builtins/basic.md#coalesce): Leave the filled values unchanged and replace `NULL` with the default value that follows
* [UNWRAP](../../builtins/basic.md#optional-ops): Extract the value of the original type from the optional data type, `T?`. is converted to `T`
* [JUST](../../builtins/basic#optional-ops): Add optionality to the current type, `T` is converted to `T?`.
* [NOTHING](../../builtins/basic.md#optional-ops): Create an empty value with the specified type.

`Optional` (nullable) isn't a property of a data type or column, but a container type where [containers](../containers.md) can be arbitrarily nested into each other. For example, a column with the type `Optional<Optional<Boolean>>` can accept 4 values: `NULL` of the whole container, `NULL` of the inner container, `TRUE`, and `FALSE`. The above-declared type differs from `List<List<Boolean>>`, because it uses `NULL` as an empty list, and you can't put more than one non-null element in it. In addition, `Optional<Optional<T>>` type values are returned as results when searching by the key in the `Dict(k,v)` dictionary with `Optional<T>` type values. Using this type of result data, you can distinguish between a `NULL` value in the dictionary and a situation when the key is missing.

{% if backend_name == "YDB" %}
{% note info %}

Container types (including `Optional<T>` containers and more complex types derived from them) can't currently be used as column data types when creating {{ ydb-short-name }} tables.
YQL queries can return values of container types and accept them as input parameters.

{% endnote %}
{% endif %}

**Example**

```sql
$dict = {"a":1, "b":null};
$found = $dict["b"];
select if($found is not null, unwrap($found), -1);
```

Result:

```text
# column0
null
```

## Logical and arithmetic operations with NULL {#null_expr}

The `NULL` literal has a separate singular `Null` type and can be implicitly converted to any optional type (for example, the nested type `Optional<Optional<...Optional<T>...>>`). In ANSI SQL, `NULL` means "an unknown value", that's why logical and arithmetic operations involving `NULL` or empty `Optional` have certain specifics.

**Examples**
```
SELECT
    True OR NULL,        -- Just(True) (works the same way as True OR <unknown value of type Bool>)
    False AND NULL,      -- Just(False)
    True AND NULL,       -- NULL   (more precise than Nothing<Bool?> – <unknown value of type Bool>)
    NULL OR NOT NULL,    -- NULL   (all NULLs are "different")
    1 + NULL,            -- NULL   (Nothing<Int32?>) - the result of adding 1 together with
                         --         unknown value of type Int)
    1 == NULL,           -- NULL   (the result of adding 1 together with unknown value of type Int)
    (1, NULL) == (1, 2), -- NULL   (composite elements are compared by component
                         --         through `AND`)
    (2, NULL) == (1, 3), -- Just(False) (expression is equivalent to 2 == 1 AND NULL == 3)

```
