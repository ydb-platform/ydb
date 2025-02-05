# PROCESS

Convert the input table using a UDF or a [lambda function](expressions.md#lambda) that is applied sequentially to each input row and can create zero to create one or more result row for each input row (similarly to Map in MapReduce terms).

The parameters of the function call after the `USING` keyword explicitly specify the values of columns from which to pass values for each input row and in which order.

You can use functions that return the result of one of three composite types derived from `OutputType` (supported `OutputType` options are described below):

* `OutputType`: Each input row must always have an output row with the schema determined by the structure type.
* `OutputType?`: The function may skip rows, returning empty values (`TUnboxedValue()` in C++, `None` in Python, or `null` in JavaScript).
* `Stream<OutputType>` or `List<OutputType>`: An option to return multiple rows.

Regardless of the option selected above, the result is converted to a flat table with columns defined by the `OutputType` type.

As `OutputType`, you can use one of the types:

* `Struct<...>`: `PROCESS` has exactly one output with entries of a given structure that is a flat table with columns corresponding to the fields `Struct<...>`
* `Variant<Struct...>,...>`: `PROCESS` will have the number of outputs equal to the number of variants in `Variant`. The entries of each output are represented by a flat table with columns based on fields from the relevant variant. In this case, you can access the set of `PROCESS` outputs as a `Tuple` of lists that can be unpacked into separate [named expressions](expressions.md#named-nodes) and used independently.

In the list of function arguments after the `USING` keyword, you can pass one of the two special named expressions:

* `TableRow()`: The entire current row in the form of a structure.
* `TableRows()`: A lazy iterator by strings, in terms of the types `Stream<Struct...>>`. In this case, the output type of the function can only be `Stream<OutputType>` or `List<OutputType>`.

{% note info %}

After executing `PROCESS`, within the same query, on the resulting table (or tables), you can perform [SELECT](select/index.md), [REDUCE](reduce.md), other `PROCESS` and so on, depending on the intended result.

{% endnote %}

The `USING` keyword and the function are optional: if you omit them, the source table is returned. This can be convenient for using a [subquery template](subquery.md).

In `PROCESS` you can pass multiple inputs (the input here means a table, a [range of tables](select/concat.md), a subquery, a [named expression](expressions.md#named-nodes)), separated by commas. To the function from `USING`, you can only pass in this case special named expressions `TableRow()` or  `TableRows()` that will have the following type:

* `TableRow()`: A `Variant` where each element has an entry structure type from the relevant input. For each input row in the Variant, the element corresponding to the occurrence ID for this row is non-empty
* `TableRows()`: A lazy iterator by Variants, in terms of the types `Stream<Variant...>>`. The alternative has the same semantics as for `TableRow()`

After `USING` in `PROCESS` you can optionally specify `ASSUME ORDER BY` with a list of columns. The result of such a  `PROCESS` statement is treated as sorted, but without actually running a sort. Sort check is performed at the query execution stage. It supports setting the sort order using the keywords `ASC` (ascending order) and `DESC` (descending order). Expressions are not supported in `ASSUME ORDER BY`.

## Examples

```yql
PROCESS my_table
USING MyUdf::MyProcessor(value)
```

```yql
$udfScript = @@
def MyFunc(my_list):
    return [(int(x.key) % 2, x) for x in my_list]
@@;

-- The function returns an iterator of Variants
$udf = Python3::MyFunc(Callable<(Stream<Struct<...>>) -> Stream<Variant<Struct<...>, Struct<...>>>>,
    $udfScript
);

-- The output of the PROCESS produces a tuple of lists
$i, $j = (PROCESS my_table USING $udf(TableRows()));

SELECT * FROM $i;
SELECT * FROM $j;
```

```yql
$udfScript = @@
def MyFunc(stream):
    for r in stream:
        yield {"alt": r[0], "key": r[1].key}
@@;

-- The function accepts an iterator of Variants as input
$udf = Python::MyFunc(Callable<(Stream<Variant<Struct<...>, Struct<...>>>) -> Stream<Struct<...>>>,
    $udfScript
);

PROCESS my_table1, my_table2 USING $udf(TableRows());
```
