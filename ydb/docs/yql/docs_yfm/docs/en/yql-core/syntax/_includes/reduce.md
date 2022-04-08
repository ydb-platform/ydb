# REDUCE

Groups the input by the specified key columns, then passes the current keys and a lazy iterator over their corresponding values from the remaining columns to the specified UDF for processing. Similarly to `PROCESS`, the UDF can return an arbitrary number of result rows per call and also return `Variants` to create multiple outputs. In terms of MapReduce, it's very similar to Reduce.

Keywords that can follow:

* `PRESORT` <span style="color: gray;">(optional)</span>: Specifying the order within each group, the syntax is similar to [ORDER BY](../select.md#orderby);
* `ON` <span style="color: gray;">(required)</span>: Specifying key columns.
* `USING` or `USING ALL` <span style="color: gray;">(required)</span>: A UDF call, see more about the rules below.

Rules for passing UDF arguments:

* If `TableRows()` is specified as a UDF argument, then the UDF must accept one argument: a lazy iterator over strings, with the type `Stream<Struct...>>`. In this case, the output type of the function can only be `Stream<OutputType>` or `List<OutputType>`. It's guaranteed that the data in the input iterator is grouped by the key and, if necessary, sorted according to the `PRESORT` section. With `TableRows()`, you can only use `USING ALL`.
* With `USING`:
    * The UDF must accept two arguments: the current key is passed to the first argument, and a lazy iterator with values corresponding to this key is passed to the second argument.
* With `USING ALL`:
    * The UDF must accept one argument: a lazy iterator over `Tuples`, where the first item in the tuple is a key, and the second item is a lazy iterator with values corresponding to this key.
* The key to be passed to the UDF follows the rule below. If there is only one key column, then only its value is used in the key. If there are multiple columns (columns are listed similarly to `GROUP BY` separated by commas), then the key is a `Tuple` with values from the listed columns in the specified order.
* When you call `REDUCE` from a query, only the expression whose values will be passed as iterator items follows the UDF name in parentheses (the second UDF argument for `USING` or the second item of the tuple for `USING ALL`).

The result is built in the same way as for [PROCESS](../process.md). You can also use the `TableRow()` keyword to get the whole string as a structure.

In `REDUCE`, you can pass multiple inputs (the input here means a table, a [range of tables](../select.md#range), a subquery, a [named expression](../expressions.md#named-nodes)), separated by commas. All inputs must have the key columns of the matching type specified in `ON`. To the function from `USING` in this case, you can only pass a special named expression `TableRow()`. The second argument (or the second element of the tuple for `USING ALL`) will include a lazy iterator over variants with a populated element that corresponds to the occurrence ID for the current entry.

After `USING`, in `REDUCE` you can optionally specify `ASSUME ORDER BY` with a list of columns. The result of such a `REDUCE` statement is treated as sorted, but without actually running a sort. Sort check is performed at the query execution stage. It supports setting the sort order using the keywords `ASC` (ascending order) and `DESC` (descending order). Expressions are not supported in `ASSUME ORDER BY`.

**Examples:**

```yql
REDUCE my_table
ON key, subkey
USING MyUdf::MyReducer(TableRow());
```

```yql
REDUCE my_table
ON key, subkey
USING ALL MyUDF::MyStreamReducer(TableRow()); -- MyUDF::MyStreamReducer accepts a lazy list of tuples (key, list of entries for the key) as its input
```

```yql
REDUCE my_table
PRESORT LENGTH(subkey) DESC
ON key
USING MyUdf::MyReducer(
    AsTuple(subkey, value)
);
```

```yql
REDUCE my_table
ON key
USING ALL MyUDF::MyFlatStreamReducer(TableRows()); -- MyUDF::MyFlatStreamReducer accepts a single lazy list of entries as its input
```

```yql
-- The function returns Variants
$udf = Python::MyReducer(Callable<(String, Stream<Struct<...>>) -> Variant<Struct<...>, Struct<...>>>,
    $udfScript
);

-- The output of REDUCE produces a tuple of lists
$i, $j = (REDUCE my_table ON key USING $udf(TableRow()));

SELECT * FROM $i;
SELECT * FROM $j;
```

```yql
$script = @@
def MyReducer(key, values):
    state = None, 0
    for name, last_visit_time in values:
        if state[1] < last_visit_time:
            state = name, last_visit_time
    return {
        'region':key,
        'last_visitor':state[0],
    }
@@;

$udf = Python::MyReducer(Callable<(
    Int64?,
    Stream<Tuple<String?, Uint64?>>
) -> Struct<
    region:Int64?,
    last_visitor:String?
>>,
    $script
);

REDUCE hahn.`home/yql/tutorial/users`
ON region USING $udf((name, last_visit_time));
```

```yql
-- The function accepts a key and iterator of Variants as input
$udf = Python::MyReducer(Callable<(String, Stream<Variant<Struct<...>,Struct<...>>>) -> Struct<...>>,
    $udfScript
);

REDUCE my_table1, my_table2 ON key USING $udf(TableRow());
```

