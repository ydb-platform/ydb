# COMBINE

Groups rows from two input tables by a common key and applies a UDF or a [lambda function](expressions.md#lambda) to each group. Unlike [JOIN](join.md), `COMBINE` does not produce a Cartesian product of matching rows. Instead, it passes all rows with the same key to the function as two lists, allowing the function to implement custom matching logic.

## Syntax

```yql
COMBINE input1 AS alias1 [PRESORT presort_expression1 [ASC | DESC], ...]
WITH input2 AS alias2 [PRESORT presort_expression2 [ASC | DESC], ...]
ON alias1.key_expression = alias2.key_expression [AND ...]
USING function(item_expression1, item_expression2)
```

## Availability

`COMBINE` is available since [2026.02](../changelog/2026.02.md) language version.

## Description

The `ON` clause specifies one or more equality conditions joined by `AND`. If several predicates are specified, the result is a composite key whose component values form a tuple; otherwise, the key is scalar.

For every key present in either input, `COMBINE` calls the function with three arguments:

1. The common key specified by `ON`.
2. A list of values produced by evaluating the expression in the first `USING` argument for rows from the first input with that key.
3. A list of values produced by evaluating the expression in the second `USING` argument for rows from the second input with that key.

If a key occurs in only one input, the list for the other input is empty. Thus, `COMBINE` has `FULL JOIN` semantics at the group level.

The two expressions in `USING` specify which value is collected from each input row. Use `TableRow()` to pass the entire row as a structure. Other expressions can select only the required columns or calculate a value before it is added to the list.

The optional `PRESORT` clause sorts rows within each key group before their values are passed to the function. Each input has its own `PRESORT` clause. Sort expressions support `ASC` (the default) and `DESC`. Without `PRESORT`, the order of items in the lists is not defined.

The function specified in `USING` can return the same output types as in [PROCESS](process.md): a structure, an optional structure, or a list or stream of structures. The result is converted to a flat table. An optional value can omit the result for a group, while a list or stream can produce multiple result rows.

{% note info %}

`COMBINE` is useful when rows with the same key must be processed together but ordinary join multiplication is undesirable, for example when matching time intervals or implementing domain-specific merge logic.

{% endnote %}

## Examples

```yql
$count_rows = ($key, $left_rows, $right_rows) -> {
    RETURN <|
        key: $key,
        left_count: ListLength($left_rows),
        right_count: ListLength($right_rows)
    |>;
};

COMBINE my_table1 AS L
WITH my_table2 AS R
ON L.key = R.key
USING $count_rows(TableRow(), TableRow());
```

```yql
$zip_rows = ($key, $left_rows, $right_rows) -> {
    RETURN <|
        key: $key.0,
        subkey: $key.1,
        rows: ListZipAll($left_rows, $right_rows)
    |>;
};

COMBINE my_table1 AS L
    PRESORT L.timestamp
WITH my_table2 AS R
    PRESORT R.timestamp
ON L.key = R.key AND L.subkey = R.subkey
USING $zip_rows(TableRow(), TableRow());
```
