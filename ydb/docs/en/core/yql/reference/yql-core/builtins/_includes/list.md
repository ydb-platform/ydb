# Functions for lists

## ListCreate {#list-create}

Construct an empty list. The only argument specifies a string describing the data type of the list cell, or the type itself obtained using [relevant functions](../types.md). YQL doesn't support lists with an unknown cell type.

[Documentation for the type definition format](../../types/type_string.md).

**Examples**

```yql
SELECT ListCreate(Tuple<String,Double?>);
```

```yql
SELECT ListCreate(OptionalType(DataType("String")));
```

## AsList and AsListStrict {#aslist}

Construct a list based on one or more arguments. The argument types must be compatible in the case of `AsList` and strictly match in the case of `AsListStrict`.

**Examples**

```yql
SELECT AsList(1, 2, 3, 4, 5);
```

## ListLength {#listlength}

The count of items in the list.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT ListLength(list_column) FROM my_table;
```

{% endif %}

## ListHasItems

Check that the list contains at least one item.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT ListHasItems(list_column) FROM my_table;
```

{% endif %}

## ListCollect {#listcollect}

Convert a lazy list (it can be built by such functions as [ListFilter](#listfilter), [ListMap](#listmap), [ListFlatMap](#listflatmap)) to an eager list. In contrast to a lazy list, where each new pass re-calculates the list contents, in an eager list the content is built at once by consuming more memory.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT ListCollect(list_column) FROM my_table;
```

{% endif %}

## ListSort, ListSortAsc, and ListSortDesc {#listsort}

Sort the list. By default, the ascending sorting order is applied (`ListSort` is an alias for `ListSortAsc`).

Arguments:

1. List.
2. An optional expression to get the sort key from a list element (it's the element itself by default).

**Examples**

{% if feature_column_container_type %}

```yql
SELECT ListSortDesc(list_column) FROM my_table;
```

{% endif %}

```yql
$list = AsList(
    AsTuple("x", 3),
    AsTuple("xx", 1),
    AsTuple("a", 2)
);

SELECT ListSort($list, ($x) -> {
    RETURN $x.1;
});
```

{% note info %}

The example used a [lambda function](../../syntax/expressions.md#lambda).

{% endnote %}

## ListExtend and ListExtendStrict {#listextend}

Sequentially join lists (concatenation of lists). The arguments can be lists, optional lists, and `NULL`.
The types of list items must be compatible in the case of `ListExtend` and strictly match in the case of `ListExtendStrict`.
If at least one of the lists is optional, then the result is also optional.
If at least one argument is `NULL`, then the result type is `NULL`.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT ListExtend(
    list_column_1,
    list_column_2,
    list_column_3
) FROM my_table;
```

{% endif %}

## ListUnionAll {#listunionall}

Sequentially join lists of structures (concatenation of lists). A field is added to the output list of structures if it exists in at least one source list, but if there is no such field in any list, it is added as NULL. In the case when a field is present in two or more lists, the output field is cast to the common type.

If at least one of the lists is optional, then the result is also optional.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT ListUnionAll(
    list_column_1,
    list_column_2,
    list_column_3
) FROM my_table;
```

{% endif %}

## ListZip and ListZipAll {#listzip}

Based on the input lists, build a list of pairs containing the list items with matching indexes (`List<Tuplefirst_list_element_type,second_list_element_type>`).

The length of the returned list is determined by the shortest list for ListZip and the longest list for ListZipAll.
When the shorter list is exhausted, a `NULL` value of a relevant [optional type](../../types/optional.md) is paired with the elements of the longer list.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT
    ListZip(list_column_1, list_column_2, list_column_3),
    ListZipAll(list_column_1, list_column_2)
FROM my_table;
```

{% endif %}

## ListEnumerate {#listenumerate}

Build a list of pairs (Tuple) containing the element number and the element itself (`List<TupleUint64,list_element_type>`).

**Examples**
{% if feature_column_container_type %}

```yql
SELECT ListEnumerate(list_column) FROM my_table;
```

{% endif %}

## ListReverse {#listreverse}

Reverse the list.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT ListReverse(list_column) FROM my_table;
```

{% endif %}

## ListSkip {#listskip}

Returns a copy of the list, skipping the specified number of its first elements.

The first argument specifies the source list and the second argument specifies how many elements to skip.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT
    ListSkip(list_column, 3)
FROM my_table;
```

{% endif %}

## ListTake {#listtake}

Returns a copy of the list containing a limited number of elements from the second list.

The first argument specifies the source list and the second argument specifies the maximum number of elements to be taken from the beginning of the list.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT ListTake(list_column, 3) FROM my_table;
```

{% endif %}

## ListIndexOf {#listindexof}

Searches the list for an element with the specified value and returns its index at the first occurrence. Indexes count from 0. If such element is missing, it returns `NULL`.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT
    ListIndexOf(list_column, 123)
FROM my_table;
```

{% endif %}

## ListMap, ListFilter, and ListFlatMap {#listmap}

Apply the function specified as the second argument to each list element. The functions differ in their returned result:

* `ListMap` returns a list with results.
* `ListFlatMap` returns a list with results, combining and expanding the first level of results (lists or optional values) for each item.
* `ListFilter` leaves only those elements where the function returned `true`.

{% note info %}

In `ListFlatMap`, using optional values in function results is deprecated, use the combination of [`ListNotNull`](#listnotnull) and `ListMap` instead.

{% endnote %}

Arguments:

1. Source list.
2. Functions for processing list elements, such as:
    * [Lambda function](../../syntax/expressions.md#lambda).
    * `Module::Function` - C++ UDF.
{% if feature_udf_noncpp %}
    * [Python UDF](../../udf/python.md), [JavaScript UDF](../../udf/javascript.md) or any other called value.

If the source list is optional, then the output list is also optional.

**Examples**
{% if feature_column_container_type %}

```yql
$callable = Python::test(Callable<(Int64)->Bool>, "defMyFavouriteCrutchtest(i): return i % 2");
SELECT
    ListMap(list_column, ($x) -> { RETURN $x > 2; }),
    ListFlatMap(list_column, My::Udf),
    ListFilter(list_column, $callable)
FROM my_table;
```

{% endif %}
{% endif %}

## ListNotNull {#listnotnull}

Applies transformation to the source list, skipping empty optional items and strengthening the item type to non-optional. For a list with non-optional items, it returns the unchanged source list.

If the source list is optional, then the output list is also optional.

**Examples**

```yql
SELECT ListNotNull([1,2]),   -- [1,2]
    ListNotNull([3,null,4]); -- [3,4]
```

## ListFlatten {#listflatten}

Expands the list of lists into a flat list, preserving the order of items. As the top-level list item you can use an optional list that is interpreted as an empty list in the case of `NULL`.

If the source list is optional, then the output list is also optional.

**Examples**

```yql
SELECT ListFlatten([[1,2],[3,4]]),   -- [1,2,3,4]
    ListFlatten([null,[3,4],[5,6]]); -- [3,4,5,6]
```

## ListUniq {#listuniq}

Returns a copy of the list containing only distinct elements.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT
    ListUniq(list_column)
FROM my_table;
```

{% endif %}

## ListAny and ListAll {#listany}

Returns `true` for a list of Boolean values, if:

* `ListAny`: At least one element is `true`.
* `ListAll`: All elements are `true`.

Otherwise, it returns false.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT
    ListAll(bool_column),
    ListAny(bool_column)
FROM my_table;
```

{% endif %}

## ListHas {#listhas}

Show whether the list contains the specified element.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT
    ListHas(list_column, "my_needle")
FROM my_table;
```

{% endif %}

## ListHead, ListLast {#listheadlast}

Returns the first and last item of the list.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT
    ListHead(numeric_list_column) AS head,
    ListLast(numeric_list_column) AS last
FROM my_table;
```

{% endif %}

## ListMin, ListMax, ListSum and ListAvg {#listminy}

Apply the appropriate aggregate function to all elements of the numeric list.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT
    ListMax(numeric_list_column) AS max,
    ListMin(numeric_list_column) AS min,
    ListSum(numeric_list_column) AS sum,
    ListAvg(numeric_list_column) AS avg
FROM my_table;
```

{% endif %}

## ListFold, ListFold1 {#listfold}

Folding a list.

Arguments:

1. List
2. Initial state U for ListFold, initLambda(item:T)->U for ListFold1
3. updateLambda(item:T, state:U)->U

Type returned:
U for ListFold, optional U for ListFold1.

**Examples**

```yql
$l = [1, 4, 7, 2];
$y = ($x, $y) -> { RETURN $x + $y; };
$z = ($x) -> { RETURN 4 * $x; };

SELECT
    ListFold($l, 6, $y) AS fold,                       -- 20
    ListFold([], 3, $y) AS fold_empty,                 -- 3
    ListFold1($l, $z, $y) AS fold1,                    -- 17
    ListFold1([], $z, $y) AS fold1_empty;              -- Null
```

## ListFoldMap, ListFold1Map {#listfoldmap}

Converts each list item i by calling the handler(i, state).

Arguments:

1. List
2. Initial state S for ListFoldMap, initLambda(item:T)->tuple (U S) for ListFold1Map
3. handler(item:T, state:S)->tuple (U S)

Type returned: List of U items.

**Examples**

```yql
$l = [1, 4, 7, 2];
$x = ($i, $s) -> { RETURN ($i * $s, $i + $s); };
$t = ($i) -> { RETURN ($i + 1, $i + 2); };

SELECT
    ListFoldMap([], 1, $x),                -- []
    ListFoldMap($l, 1, $x),                -- [1, 8, 42, 26]
    ListFold1Map([], $t, $x),              -- []
    ListFold1Map($l, $t, $x);              -- [2, 12, 49, 28]
```

## ListFromRange {#listfromrange}

Generate a sequence of numbers with the specified step. It's similar to `xrange` in Python 2, but additionally supports floats.

Arguments:

1. Start
2. End
3. Step (optional, 1 by default)

Specifics:

* The end is not included, i.e. `ListFromRange(1,3) == AsList(1,2)`.
* The type for the resulting elements is selected as the broadest from the argument types. For example, `ListFromRange(1, 2, 0.5)` results in a `Double` list.
* The list is "lazy", but if it's used incorrectly, it can still consume a lot of RAM.
* If the step is positive and the end is less than or equal to the start, the result list is empty.
* If the step is negative and the end is greater than or equal to the start, the result list is empty.
* If the step is neither positive nor negative (0 or NaN), the result list is empty.

**Examples**

```yql
SELECT
    ListFromRange(-2, 2), -- [-2, -1, 0, 1]
    ListFromRange(2, 1, -0.5); -- [2.0, 1.5]
```

## ListReplicate {#listreplicate}

Creates a list containing multiple copies of the specified value.

Required arguments:

1. Value.
2. Number of copies.

**Examples**

```yql
SELECT ListReplicate(true, 3); -- [true, true, true]
```

## ListConcat {#listconcat}

Concatenates a list of strings into a single string.
You can set a separator as the second parameter.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT
    ListConcat(string_list_column),
    ListConcat(string_list_column, "; ")
FROM my_table;
```

{% endif %}

## ListExtract {#listextract}

For a list of structures, it returns a list of contained fields having the specified name.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT
    ListExtract(struct_list_column, "MyMember")
FROM my_table;
```

{% endif %}

## ListTakeWhile, ListSkipWhile {#listtakewhile}

`ListTakeWhile` returns a list from the beginning while the predicate is true, then the list ends.

`ListSkipWhile` skips the list segment from the beginning while the predicate is true, then returns the rest of the list ignoring the predicate.
`ListTakeWhileInclusive` returns a list from the beginning while the predicate is true. Then the list ends, but it also includes the item on which the stopping predicate triggered.
`ListSkipWhileInclusive` skips a list segment from the beginning while the predicate is true, then returns the rest of the list disregarding the predicate, but excluding the element that matched the predicate and starting with the next element after it.

Required arguments:

1. List.
2. Predicate.

If the input list is optional, then the result is also optional.

**Examples**

```yql
$data = AsList(1, 2, 5, 1, 2, 7);

SELECT
    ListTakeWhile($data, ($x) -> {return $x <= 3}), -- [1, 2]
    ListSkipWhile($data, ($x) -> {return $x <= 3}), -- [5, 1, 2, 7]
    ListTakeWhileInclusive($data, ($x) -> {return $x <= 3}), -- [1, 2, 5]
    ListSkipWhileInclusive($data, ($x) -> {return $x <= 3}); -- [1, 2, 7]
```

## ListAggregate {#listaggregate}

Apply the [aggregation factory](../basic.md#aggregationfactory) to the passed list.
If the passed list is empty, the aggregation result is the same as for an empty table: 0 for the `COUNT` function and `NULL` for other functions.
If the passed list is optional and `NULL`, the result is also `NULL`.

Arguments:

1. List.
2. [Aggregation factory](../basic.md#aggregationfactory).

**Examples**

```yql
SELECT ListAggregate(AsList(1, 2, 3), AggregationFactory("Sum")); -- 6
```

## ToDict and ToMultiDict {#todict}

Convert a list of tuples containing key-value pairs to a dictionary. In case of conflicting keys in the input list, `ToDict` leaves the first value and `ToMultiDict` builds a list of all the values.

It means that:

* `ToDict` converts `List<TupleK, V="">` to `Dict<K, V="">`
* `ToMultiDict` converts `List<TupleK, V>` to `Dict<K, List<V>>`

Optional lists are also supported, resulting in an optional dictionary.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT
    ToDict(tuple_list_column)
FROM my_table;
```

{% endif %}

## ToSet {#toset}

Converts a list to a dictionary where the keys are unique elements of this list, and values are omitted and have the type `Void`. For the `List<T>` list, the result type is `Dict<T, Void="">`.
An optional list is also supported, resulting in an optional dictionary.

Inverse function: get a list of keys for the [DictKeys](../dict.md#dictkeys) dictionary.

**Examples**
{% if feature_column_container_type %}

```yql
SELECT
    ToSet(list_column)
FROM my_table;
```

{% endif %}

## ListTop, ListTopAsc, ListTopDesc, ListTopSort, ListTopSortAsc и ListTopSortDesc {#listtop}

Select top values from the list. `ListTopSort*` additionally sorts the returned values. The smallest values are selected by default. Thus, the functions without a suffix are the aliases to `*Asc` functions, while `*Desc` functions return the largest values.

`ListTopSort` is more effective than consecutive `ListTop` and `ListSort` because `ListTop` can partially sort the list to find needed values. However, `ListTop` is more effective than `ListTopSort` when the result order is unimportant.

Arguments:

1. List.
2. Size of selection.
3. An optional expression to get the sort key from a list element (it's the element itself by default).

**Signature**
```
ListTop(List<T>{Flags:AutoMap}, N)->List<T>
ListTop(List<T>{Flags:AutoMap}, N, (T)->U)->List<T>
```
The signatures of other functions are the same.
