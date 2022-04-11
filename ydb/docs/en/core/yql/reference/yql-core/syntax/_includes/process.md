# {{ process_command }}

Convert the input table using a UDF in [C++](../../udf/cpp.md){% if yt %}, [Python](../../udf/python.md) or [JavaScript](../../udf/javascript.md){% endif %} or a [lambda function](../../syntax/expressions.md#lambda) that is applied sequentially to each input row and can create zero to create one or more result row for each input row (similarly to Map in MapReduce terms).

{% if feature_mapreduce %}The table is searched by name in the database specified by the [USE](../use.md) operator.{% endif %}

The parameters of the function call after the `USING` keyword explicitly specify the values of columns from which to pass values for each input row and in which order.

You can use functions that return the result of one of three composite types derived from `OutputType` (supported `OutputType` options are described below):

* `OutputType`: Each input row must always have an output row with the schema determined by the structure type.
* `OutputType?`: The function may skip rows, returning empty values (`TUnboxedValue()` in C++, `None` in Python, or `null` in JavaScript).
* `Stream<OutputType>` or `List<OutputType>`: An option to return multiple rows.

Regardless of the option selected above, the result is converted to a flat table with columns defined by the `OutputType` type.

As `OutputType`, you can use one of the types:

* `Struct<...>`: `{{ process_command }}` has exactly one output with entries of a given structure that is a flat table with columns corresponding to the fields `Struct<...>`
* `Variant<Struct...>,...>`: `{{ process_command }}` will have the number of outputs equal to the number of variants in `Variant`. The entries of each output are represented by a flat table with columns based on fields from the relevant variant. In this case, you can access the set of `{{ process_command }}` outputs as a `Tuple` of lists that can be unpacked into separate [named expressions](../expressions.md#named-nodes) and used independently.

In the list of function arguments after the `USING` keyword, you can pass one of the two special named expressions:

* `TableRow()`: The entire current row in the form of a structure.
* `TableRows()`: A lazy iterator by strings, in terms of the types `Stream<Struct...>>`. In this case, the output type of the function can only be `Stream<OutputType>` or `List<OutputType>`.

{% note info %}

After executing `{{ process_command }}`, within the same query, on the resulting table (or tables), you can perform {% if select_command != "SELECT STREAM" %}[SELECT](../select.md), [REDUCE](../reduce.md){% else %}[SELECT STREAM](../select_stream.md){% endif %}, [INSERT INTO](../insert_into.md), other `{{ process_command }}` and so on, depending on the intended result.

{% endnote %}

The `USING` keyword and the function are optional: if you omit them, the source table is returned. {% if feature_subquery %}This can be convenient for using a [subquery template](subquery.md).{% endif %}

In `{{ process_command }}` you can pass multiple inputs (the input here means a table,{% if select_command != "PROCESS STREAM" %} a [range of tables](../select.md#range){% endif %}, a subquery, a [named expression](../expressions.md#named-nodes)), separated by commas. To the function from `USING`, you can only pass in this case special named expressions `TableRow()` or  `TableRows()` that will have the following type:

* `TableRow()`: A `Variant` where each element has an entry structure type from the relevant input. For each input row in the Variant, the element corresponding to the occurrence ID for this row is non-empty
* `TableRows()`: A lazy iterator by Variants, in terms of the types `Stream<Variant...>>`. The alternative has the same semantics as for `TableRow()`

After `USING` in `{{ process_command }}` you can optionally specify `ASSUME ORDER BY` with a list of columns. The result of such a  `{{ process_command }}` statement is treated as sorted, but without actually running a sort. Sort check is performed at the query execution stage. It supports setting the sort order using the keywords `ASC` (ascending order) and `DESC` (descending order). Expressions are not supported in `ASSUME ORDER BY`.

**Examples:**

{% if process_command != "PROCESS STREAM" %}

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

{% else %}

```yql

-- The lambda function accepts a primitive type and returns a structure with three identical fields
$f = ($r) -> {
   return AsStruct($r as key, $r as subkey, $r as value);
};

-- Filtering the input stream by the WHERE clause
PROCESS STREAM Input USING $f(value) WHERE cast(subkey as int) > 3;
```

```yql
-- The lambda function accepts the type `Struct<key:String, subkey:String, value:String>`
-- and returns a similar structure by adding a suffix to each field

$f = ($r) -> {
    return AsStruct($r.key || "_1" as key, $r.subkey || "_2" as subkey, $r.value || "_3" as value);
};

PROCESS STREAM Input USING $f(TableRow());
```

```yql
-- The lambda function accepts an entry in the format `Struct<...>`
-- and returns a list of two identical items
$f1 = ($x) -> {
    return AsList($x, $x);
};

-- The lambda function accepts and returns the type `Stream<Struct...>>`
-- It applies the $f1 function to each element and returns duplicate rows
$f2 = ($x) -> {
    return ListFlatMap($x, $f1);
};

$p = (PROCESS STREAM Input USING $f2(TableRows()));

SELECT STREAM * FROM $p;
```

{% endif %}

