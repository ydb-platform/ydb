{% if backend_name == "YDB" %}

{% note info %}

Container types (including `Optional<T>` containers and more complex types derived from them) can't currently be used as column data types when creating {{ ydb-short-name }} tables.
YQL queries can return values of container types and accept them as input parameters.

{% endnote %}

{% endif %}

{% if has_create_table_link == true %}

## Data types that do not allow NULL values {#notnull}

[Primitive types](../primitive.md) in YQL cannot hold a `NULL` value: the container described above, `Optional`, is intended for storing `NULL`. In SQL terms, primitive types in YQL are *non-nullable* types.

In YQL, there is no implicit type conversion from Optional<T> to T, so the enforceability of the NOT NULL constraint on a table column is ensured at the query compilation stage in {{ ydb-short-name }}.

You can create a non-nullable column in a {{ ydb-short-name }} table using the [CREATE TABLE](../../syntax/create_table/index.md) operation with the keyword `NOT NULL`:

```yql
CREATE TABLE t (
    Key Uint64 NOT NULL,
    Value String NOT NULL,
    PRIMARY KEY (Key))
```

After that, write operations to table `t` will only be executed if the values to be inserted into the `key` and `value` columns do not contain `NULL` values.

### Example of the interaction between the NOT NULL constraint and YQL functions

Many of the YQL functions have optional types as return values. Since YQL is a strongly-typed language, a query like

```yql
CREATE TABLE t (
    c Utf8 NOT NULL,
    PRIMARY KEY (c)
);
INSERT INTO t(c)
SELECT CAST('q' AS Utf8);
```

cannot be executed. The reason for this is the type mismatch between the column `c`, which has the type `Utf8`, and the result of the `CAST` function, which has the type `Optional<Utf8>`. To make the query work correctly in such scenarios, it is necessary to use the [COALESCE](../../builtins/basic.md#coalesce) function, whose argument can specify a fallback value to insert into the table in case the function (in the example, CAST) returns an empty `Optional`. If, in the case of an empty `Optional`, the insertion should not be performed and an error should be returned, the [UNWRAP](../../builtins/basic.md#optional-ops) function can be used to unpack the contents of the optional type.

{% endif %}
