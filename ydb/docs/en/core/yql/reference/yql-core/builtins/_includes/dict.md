# Functions for dictionaries

## DictCreate {#dictcreate}

Construct an empty dictionary. Two arguments are passed: for a key and a value. Each argument specifies a string with the data type declaration or the type itself built by [type functions](../types.md). There are no dictionaries with an unknown key or value type in YQL. As a key, you can set a [primitive data type](../../types/primitive.md), except for `Yson` and `Json` that may be [optional](../../types/optional.md) or a tuple of them of a length of at least two.

[Documentation for the type definition format](../../types/type_string.md).

**Examples**

```yql
SELECT DictCreate(String, Tuple<String,Double?>);
```

```yql
SELECT DictCreate(Tuple<Int32?,String>, OptionalType(DataType("String")));
```

## SetCreate {#setcreate}

Construct an empty set. An argument is passed: the key type that can be built by [type functions](../types.md). There are no sets with an unknown key type in YQL. As a key, you can set a [primitive data type](../../types/primitive.md), except for `Yson` and `Json` that may be [optional](../../types/optional.md) or a tuple of them of a length of at least two.

[Documentation for the type definition format](../../types/type_string.md).

**Examples**

```yql
SELECT SetCreate(String);
```

```yql
SELECT SetCreate(Tuple<Int32?,String>);
```

## DictLength {#dictlength}

The count of items in the dictionary.

**Examples**

```yql
SELECT DictLength(AsDict(AsTuple(1, AsList("foo", "bar"))));
```

{% if feature_column_container_type %}

```yql
SELECT DictLength(dict_column) FROM my_table;
```

{% endif %}

## DictHasItems {#dicthasitems}

Check that the dictionary contains at least one item.

**Examples**

```yql
SELECT DictHasItems(AsDict(AsTuple(1, AsList("foo", "bar")))) FROM my_table;
```

{% if feature_column_container_type %}

```yql
SELECT DictHasItems(dict_column) FROM my_table;
```

{% endif %}

## DictItems {#dictitems}

Get dictionary contents as a list of tuples including key-value pairs (`List<Tuplekey_type,value_type>`).

**Examples**

```yql
SELECT DictItems(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ ( 1, [ "foo", "bar" ] ) ]
```

{% if feature_column_container_type %}

```yql
SELECT DictItems(dict_column)
FROM my_table;
```

{% endif %}

## DictKeys {#dictkeys}

Get a list of dictionary keys.

**Examples**

```yql
SELECT DictKeys(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ 1 ]
```

{% if feature_column_container_type %}

```yql
SELECT DictKeys(dict_column)
FROM my_table;
```

{% endif %}

## DictPayloads {#dictpayloads}

Get a list of dictionary values.

**Examples**

```yql
SELECT DictPayloads(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ [ "foo", "bar" ] ]
```

{% if feature_column_container_type %}

```yql
SELECT DictPayloads(dict_column)
FROM my_table;
```

{% endif %}

## DictLookup {#dictlookup}

Get a dictionary element by its key.

**Examples**

```yql
SELECT DictLookup(AsDict(
    AsTuple(1, AsList("foo", "bar")),
    AsTuple(2, AsList("bar", "baz"))
), 1);
-- [ "foo", "bar" ]
```

{% if feature_column_container_type %}

```yql
SELECT DictLookup(dict_column, "foo")
FROM my_table;
```

{% endif %}

## DictContains {#dictcontains}

Checking if an element in the dictionary using its key. Returns true or false.

**Examples**

```yql
SELECT DictContains(AsDict(
    AsTuple(1, AsList("foo", "bar")),
    AsTuple(2, AsList("bar", "baz"))
), 42);
-- false
```

{% if feature_column_container_type %}

```yql
SELECT DictContains(dict_column, "foo")
FROM my_table;
```

{% endif %}

## DictAggregate {#dictaggregate}

Apply [aggregation factory](../basic.md#aggregationfactory) to the passed dictionary where each value is a list. The factory is applied separately inside each key.
If the list is empty, the aggregation result is the same as for an empty table: 0 for the `COUNT` function and `NULL` for other functions.
If the list under a certain key is empty in the passed dictionary, such a key is removed from the result.
If the passed dictionary is optional and contains `NULL`, the result is also `NULL`.

Arguments:

1. Dictionary.
2. [Aggregation factory](../basic.md#aggregationfactory).

**Examples**

```sql
SELECT DictAggregate(AsDict(
    AsTuple(1, AsList("foo", "bar")),
    AsTuple(2, AsList("baz", "qwe"))),
    AggregationFactory("Max"));
-- {1 : "foo", 2 : "qwe" }
```

## SetIsDisjoint {#setisjoint}

Check that the dictionary doesn't intersect by keys with a list or another dictionary.

So there are two options to make a call:

* With the `Dict<K,V1>` and `List<K>` arguments.
* With the `Dict<K,V1>` and `Dict<K,V2>` arguments.

**Examples**

```sql
SELECT SetIsDisjoint(ToSet(AsList(1, 2, 3)), AsList(7, 4)); -- true
SELECT SetIsDisjoint(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- false
```

## SetIntersection {#setintersection}

Construct intersection between two dictionaries based on keys.

Arguments:

* Two dictionaries: `Dict<K,V1>` and `Dict<K,V2>`.
* An optional function that combines the values from the source dictionaries to construct the values of the output dictionary. If such a function has the `(K,V1,V2) -> U` type, the result type is `Dict<K,U>`. If the function is not specified, the result type is `Dict<K,Void>`, and the values from the source dictionaries are ignored.

**Examples**

```yql
SELECT SetIntersection(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- { 3 }
SELECT SetIntersection(
    AsDict(AsTuple(1, "foo"), AsTuple(3, "bar")),
    AsDict(AsTuple(1, "baz"), AsTuple(2, "qwe")),
    ($k, $a, $b) -> { RETURN AsTuple($a, $b) });
-- { 1 : ("foo", "baz") }
```

## SetIncludes {#setincludes}

Checking that the keys of the specified dictionary include all the elements of the list or the keys of the second dictionary.

So there are two options to make a call:

* With the `Dict<K,V1>` and `List<K>` arguments.
* With the `Dict<K,V1>` and `Dict<K,V2>` arguments.

**Examples**

```yql
SELECT SetIncludes(ToSet(AsList(1, 2, 3)), AsList(3, 4)); -- false
SELECT SetIncludes(ToSet(AsList(1, 2, 3)), ToSet(AsList(2, 3))); -- true
```

## SetUnion {#setunion}

Constructs a union of two dictionaries based on keys.

Arguments:

* Two dictionaries: `Dict<K,V1>` and `Dict<K,V2>`.
* An optional function that combines the values from the source dictionaries to construct the values of the output dictionary. If such a function has the `(K,V1?,V2?) -> U` type, the result type is `Dict<K,U>`. If the function is not specified, the result type is `Dict<K,Void>`, and the values from the source dictionaries are ignored.

**Examples**

```yql
SELECT SetUnion(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- { 1, 2, 3, 4 }
SELECT SetUnion(
    AsDict(AsTuple(1, "foo"), AsTuple(3, "bar")),
    AsDict(AsTuple(1, "baz"), AsTuple(2, "qwe")),
    ($k, $a, $b) -> { RETURN AsTuple($a, $b) });
-- { 1 : ("foo", "baz"), 2 : (null, "qwe"), 3 : ("bar", null) }
```

## SetDifference {#setdifference}

Construct a dictionary containing all the keys with their values in the first dictionary with no matching key in the second dictionary.

**Examples**

```yql
SELECT SetDifference(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- { 1, 2 }
SELECT SetDifference(
    AsDict(AsTuple(1, "foo"), AsTuple(2, "bar")),
    ToSet(AsList(2, 3)));
-- { 1 : "foo" }
```

## SetSymmetricDifference {#setsymmetricdifference}

Construct a symmetric difference between two dictionaries based on keys.

Arguments:

* Two dictionaries: `Dict<K,V1>` and `Dict<K,V2>`.
* An optional function that combines the values from the source dictionaries to construct the values of the output dictionary. If such a function has the `(K,V1?,V2?) -> U` type, the result type is `Dict<K,U>`. If the function is not specified, the result type is `Dict<K,Void>`, and the values from the source dictionaries are ignored.

**Examples**

```yql
SELECT SetSymmetricDifference(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- { 1, 2, 4 }
SELECT SetSymmetricDifference(
    AsDict(AsTuple(1, "foo"), AsTuple(3, "bar")),
    AsDict(AsTuple(1, "baz"), AsTuple(2, "qwe")),
    ($k, $a, $b) -> { RETURN AsTuple($a, $b) });
-- { 2 : (null, "qwe"), 3 : ("bar", null) }
```

