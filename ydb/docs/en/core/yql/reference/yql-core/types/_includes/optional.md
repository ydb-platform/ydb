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

{% note info %}

Container types (including `Optional<T>` containers and more complex types derived from them) can't currently be used as column data types when creating {{ ydb-short-name }} tables.
YQL queries can return values of container types and accept them as input parameters.

{% endnote %}

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
{% if has_create_table_link == true %}

## Data types that do not allow NULL values {#notnull}

[Primitive types](../primitive.md) in YQL cannot hold a `NULL` value: the container described above, `Optional`, is intended for storing `NULL`. In SQL terms, primitive types in YQL are _non-nullable_ types.

In YQL, there is no implicit type conversion from Optional<T> to T, so the enforceability of the NOT NULL constraint on a table column is ensured at the query compilation stage in {{ ydb-short-name }}.

You can create a non-nullable column in a {{ ydb-short-name }} table using the [CREATE TABLE](../../../reference/syntax/create_table/index.md) operation with the keyword `NOT NULL`:
```sql
CREATE TABLE t (
    Key Uint64 NOT NULL,
    Value String NOT NULL,
    PRIMARY KEY (Key))
```

After that, write operations to table `t` will only be executed if the values to be inserted into the `key` and `value` columns do not contain `NULL` values.

### Example of the interaction between the NOT NULL constraint and YQL functions.

Many of the YQL functions have optional types as return values. Since YQL is a strongly-typed language, a query like
```sql
CREATE TABLE t (
    c Utf8 NOT NULL,
    PRIMARY KEY (c)
);
INSERT INTO t(c)
SELECT CAST('q' AS Utf8);
```
cannot be executed. The reason for this is the type mismatch between the column `c`, which has the type `Utf8`, and the result of the `CAST` function, which has the type `Optional<Utf8>`. To make the query work correctly in such scenarios, it is necessary to use the [COALESCE](../../builtins/basic.md#coalesce) function, whose argument can specify a fallback value to insert into the table in case the function (in the example, CAST) returns an empty `Optional`. If, in the case of an empty `Optional`, the insertion should not be performed and an error should be returned, the [UNWRAP](../../builtins/basic.md#optional-ops) function can be used to unpack the contents of the optional type.
{% endif %}