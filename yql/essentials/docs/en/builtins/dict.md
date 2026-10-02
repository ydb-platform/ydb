# Functions for dictionaries

## DictCreate {#dictcreate}

Construct an empty dictionary. Two arguments are passed: for a key and a value. Each argument specifies a string with the data type declaration or the type itself built by [type functions](types.md). There are no dictionaries with an unknown key or value type in YQL. As a key, you can set a [primitive data type](../types/primitive.md), except for `Yson` and `Json` that may be [optional](../types/optional.md) or a tuple of them of a length of at least two.

[Documentation for the type definition format](../types/type_string.md).

#### Examples

```yql
SELECT DictCreate(String, Tuple<String,Double?>);
```

```yql
SELECT DictCreate(Tuple<Int32?,String>, OptionalType(DataType("String")));
```

## SetCreate {#setcreate}

Construct an empty set. An argument is passed: the key type that can be built by [type functions](types.md). There are no sets with an unknown key type in YQL. As a key, you can set a [primitive data type](../types/primitive.md), except for `Yson` and `Json` that may be [optional](../types/optional.md) or a tuple of them of a length of at least two.

[Documentation for the type definition format](../types/type_string.md).

#### Examples

```yql
SELECT SetCreate(String);
```

```yql
SELECT SetCreate(Tuple<Int32?,String>);
```

## DictLength {#dictlength}

The count of items in the dictionary.

#### Examples

```yql
SELECT DictLength(AsDict(AsTuple(1, AsList("foo", "bar"))));
```

```yql
SELECT DictLength(dict_column) FROM my_table;
```

## DictHasItems {#dicthasitems}

Check that the dictionary contains at least one item.

#### Examples

```yql
SELECT DictHasItems(AsDict(AsTuple(1, AsList("foo", "bar")))) FROM my_table;
```

```yql
SELECT DictHasItems(dict_column) FROM my_table;
```


## DictItems {#dictitems}

Get dictionary contents as a list of tuples including key-value pairs (`List<Tuplekey_type,value_type>`).

#### Examples

```yql
SELECT DictItems(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ ( 1, [ "foo", "bar" ] ) ]
```


```yql
SELECT DictItems(dict_column)
FROM my_table;
```


## DictKeys {#dictkeys}

Get a list of dictionary keys.

#### Examples

```yql
SELECT DictKeys(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ 1 ]
```


```yql
SELECT DictKeys(dict_column)
FROM my_table;
```


## DictPayloads {#dictpayloads}

Get a list of dictionary values.

#### Examples

```yql
SELECT DictPayloads(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ [ "foo", "bar" ] ]
```

```yql
SELECT DictPayloads(dict_column)
FROM my_table;
```

## DictLookup {#dictlookup}

Get a dictionary element by its key.

#### Examples

```yql
SELECT DictLookup(AsDict(
    AsTuple(1, AsList("foo", "bar")),
    AsTuple(2, AsList("bar", "baz"))
), 1);
-- [ "foo", "bar" ]
```

```yql
SELECT DictLookup(dict_column, "foo")
FROM my_table;
```

## DictContains {#dictcontains}

Checking if an element in the dictionary using its key. Returns true or false.

#### Examples

```yql
SELECT DictContains(AsDict(
    AsTuple(1, AsList("foo", "bar")),
    AsTuple(2, AsList("bar", "baz"))
), 42);
-- false
```

```yql
SELECT DictContains(dict_column, "foo")
FROM my_table;
```


## DictAggregate {#dictaggregate}

Apply [aggregation factory](basic.md#aggregationfactory) to the passed dictionary where each value is a list. The factory is applied separately inside each key.
If the list is empty, the aggregation result is the same as for an empty table: 0 for the `COUNT` function and `NULL` for other functions.
If the list under a certain key is empty in the passed dictionary, such a key is removed from the result.
If the passed dictionary is optional and contains `NULL`, the result is also `NULL`.

Arguments:

1. Dictionary.
2. [Aggregation factory](basic.md#aggregationfactory).

#### Examples

```yql
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

#### Examples

```yql
SELECT SetIsDisjoint(ToSet(AsList(1, 2, 3)), AsList(7, 4)); -- true
SELECT SetIsDisjoint(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- false
```

## SetIntersection {#setintersection}

Construct intersection between two dictionaries based on keys.

Arguments:

* Two dictionaries: `Dict<K,V1>` and `Dict<K,V2>`.
* An optional function that combines the values from the source dictionaries to construct the values of the output dictionary. If such a function has the `(K,V1,V2) -> U` type, the result type is `Dict<K,U>`. If the function is not specified, the result type is `Dict<K,Void>`, and the values from the source dictionaries are ignored.

#### Examples

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

#### Examples

```yql
SELECT SetIncludes(ToSet(AsList(1, 2, 3)), AsList(3, 4)); -- false
SELECT SetIncludes(ToSet(AsList(1, 2, 3)), ToSet(AsList(2, 3))); -- true
```

## SetUnion {#setunion}

Constructs a union of two dictionaries based on keys.

Arguments:

* Two dictionaries: `Dict<K,V1>` and `Dict<K,V2>`.
* An optional function that combines the values from the source dictionaries to construct the values of the output dictionary. If such a function has the `(K,V1?,V2?) -> U` type, the result type is `Dict<K,U>`. If the function is not specified, the result type is `Dict<K,Void>`, and the values from the source dictionaries are ignored.

#### Examples

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

#### Examples

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

#### Examples

```yql
SELECT SetSymmetricDifference(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- { 1, 2, 4 }
SELECT SetSymmetricDifference(
    AsDict(AsTuple(1, "foo"), AsTuple(3, "bar")),
    AsDict(AsTuple(1, "baz"), AsTuple(2, "qwe")),
    ($k, $a, $b) -> { RETURN AsTuple($a, $b) });
-- { 2 : (null, "qwe"), 3 : ("bar", null) }
```

## DictInsert {#dictinsert}

#### Signature

```yql
DictInsert(Dict<K,V>,K,V)->Dict<K,V>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Returns a new dictionary with the specified key and value added. If the key already exists, the dictionary is not modified.
When working with a `Set`, the `Void()` function should be passed as the value.

#### Examples

```yql
SELECT DictInsert({'foo':1}, 'bar', 2); -- {'foo':1,'bar':2}
SELECT DictInsert({'foo':1}, 'foo', 2); -- {'foo':1}
SELECT DictInsert({'foo'}, 'bar', Void()); -- {'foo','bar'}
```

## DictUpsert {#dictupsert}

#### Signature

```yql
DictUpsert(Dict<K,V>,K,V)->Dict<K,V>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Returns a new dictionary with the specified key and value added or replaced. If the key already exists, the value is updated.

#### Examples

```yql
SELECT DictUpsert({'foo':1}, 'bar', 2); -- {'foo':1,'bar':2}
SELECT DictUpsert({'foo':1}, 'foo', 2); -- {'foo':2}
```

## DictUpdate {#dictupdate}

#### Signature

```yql
DictUpdate(Dict<K,V>,K,V)->Dict<K,V>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Returns a new dictionary with the replaced value of the given key. If the key does not exist, the dictionary is not modified.

#### Examples

```yql
SELECT DictUpdate({'foo':1}, 'bar', 2); -- {'foo':1}
SELECT DictUpdate({'foo':1}, 'foo', 2); -- {'foo':2}
```

## DictRemove {#dictremove}

#### Signature

```yql
DictRemove(Dict<K,V>,K)->Dict<K,V>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Returns a new dictionary without the given key. If the key does not exist, the dictionary is not modified.

#### Examples

```yql
SELECT DictRemove({'foo':1}, 'bar'); -- {'foo':1}
SELECT DictRemove({'foo':1}, 'foo'); -- {}
```

## ToMutDict {#tomutdict}

#### Signature

```yql
ToMutDict(Dict<K,V>,dependArg1...)->Linear<mutDictType for Dict<K,V>>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Converts a dictionary to its mutable version. One or more dependent expressions must also be passed, for example, using the `lambda` argument in the [`Block`](basic.md#block) function.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        return FromMutDict($dict);
    }); -- {'foo':1}
```

## MutDictCreate {#mutdictcreate}

#### Signature

```yql
MutDictCreate(KeyType,ValueType,dependArg1...)->Linear<mutDictType for Dict<K,V>>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Builds an empty mutable dictionary with the specified key and value types. You must also pass one or more dependent expressions, for example, by using the `lambda` argument in the [`Block`](basic.md#block) function.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        return FromMutDict($dict);
    }); -- {}
```

## FromMutDict {#frommutdict}

#### Signature

```yql
FromMutDict(Linear<mutDictType for Dict<K,V>>)->Dict<K,V>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Consumes a mutable dictionary and converts it to an immutable one.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        return FromMutDict($dict);
    }); -- {'foo':1}
```

## MutDictInsert {#mutdictinsert}

#### Signature

```yql
MutDictInsert(Linear<mutDictType for Dict<K,V>>,K,V)->Linear<mutDictType for Dict<K,V>>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Adds the specified key and value to a mutable dictionary and returns the same mutable dictionary. If the key already exists in the dictionary, the dictionary is not modified.
When working with a `Set`, the `Void()` function should be passed as the value.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictInsert($dict,'foo',2);
        return FromMutDict($dict);
    }); -- {'foo':1}

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictInsert($dict,'bar',2);
        return FromMutDict($dict);
    }); -- {'foo':1,'bar':2}

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo'}, $arg);
        $dict = MutDictInsert($dict,'bar', Void());
        return FromMutDict($dict);
    }); -- {'foo','bar'}
```

## MutDictUpsert {#mutdictupsert}

#### Signature

```yql
MutDictUpsert(Linear<mutDictType for Dict<K,V>>,K,V)->Linear<mutDictType for Dict<K,V>>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Adds or replaces the specified key and value in a mutable dictionary and returns the same mutable dictionary. If the key already exists in the dictionary, the value is updated.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictUpsert($dict,'foo',2);
        return FromMutDict($dict);
    }); -- {'foo':2}

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictUpsert($dict,'bar',2);
        return FromMutDict($dict);
    }); -- {'foo':1,'bar':2}
```

## MutDictUpdate {#mutdictupdate}

#### Signature

```yql
MutDictUpdate(Linear<mutDictType for Dict<K,V>>,K,V)->Linear<mutDictType for Dict<K,V>>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Replaces the value in a mutable dictionary with the specified key and returns the same mutable dictionary. If the key does not exist in the dictionary, the dictionary is not modified.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictUpdate($dict,'foo',2);
        return FromMutDict($dict);
    }); -- {'foo':2}

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictUpdate($dict,'bar',2);
        return FromMutDict($dict);
    }); -- {'foo':1}
```

## MutDictRemove {#mutdictremove}

#### Signature

```yql
MutDictRemove(Linear<mutDictType for Dict<K,V>>,K)->Linear<mutDictType for Dict<K,V>>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Removes the value from a mutable dictionary by the given key and returns the same mutable dictionary. If the key does not exist in the dictionary, the dictionary is not modified.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictRemove($dict,'foo');
        return FromMutDict($dict);
    }); --{}

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictRemove($dict,'bar');
        return FromMutDict($dict);
    }); -- {'foo':1}
```

## MutDictPop {#mutdictpop}

#### Signature

```yql
MutDictPop(Linear<mutDictType for Dict<K,V>>,K)->Tuple<Linear<mutDictType for Dict<K,V>>,V?>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Removes the value from a mutable dictionary by the given key and returns the same mutable dictionary and the value by the removed key. If the key did not exist in the dictionary, the dictionary is not modified and an empty Optional is returned.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictPop($dict,'foo');
        return (FromMutDict($dict), $val);
    }); -- ({},1)

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictPop($dict,'bar');
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},null)
```

## MutDictContains {#mutdictcontains}

#### Signature

```yql
MutDictContains(Linear<mutDictType for Dict<K,V>>,K)->Tuple<Linear<mutDictType for Dict<K,V>>,Bool>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Checks for the existence of a key in a mutable dictionary, returns the same mutable dictionary and the result.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictContains($dict,'foo');
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},True)

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictContains($dict,'bar');
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},False)
```

## MutDictLookup {#mutdictlookup}

#### Signature

```yql
MutDictLookup(Linear<mutDictType for Dict<K,V>>,K)->Tuple<Linear<mutDictType for Dict<K,V>>,V?>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Gets a value by key in a mutable dictionary, returns the same mutable dictionary and an optional result.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictLookup($dict,'foo');
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},1)

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictLookup($dict,'bar');
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},null)
```

## MutDictHasItems {#mutdicthasitems}

#### Signature

```yql
MutDictHasItems(Linear<mutDictType for Dict<K,V>>)->Tuple<Linear<mutDictType for Dict<K,V>>,Bool>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Checks whether a mutable dictionary is not empty and returns the same mutable dictionary and the result.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictHasItems($dict);
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},True)

SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        $dict, $val = MutDictHasItems($dict);
        return (FromMutDict($dict), $val);
    }); -- ({},False)
```

## MutDictLength {#mutdictlength}

#### Signature

```yql
MutDictLength(Linear<mutDictType for Dict<K,V>>)->Tuple<Linear<mutDictType for Dict<K,V>>,Uint64>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Gets the number of elements in a mutable dictionary and returns the same mutable dictionary and the result.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictLength($dict);
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},1)

SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        $dict, $val = MutDictLength($dict);
        return (FromMutDict($dict), $val);
    }); -- ({},0)
```

## MutDictKeys {#mutdictkeys}

#### Signature

```yql
MutDictKeys(Linear<mutDictType for Dict<K,V>>)->Tuple<Linear<mutDictType for Dict<K,V>>,List<K>>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Gets a list of keys in a mutable dictionary and returns the same mutable dictionary and the result.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictKeys($dict);
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},['foo'])

SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        $dict, $val = MutDictKeys($dict);
        return (FromMutDict($dict), $val);
    }); -- ({},[])
```

## MutDictPayloads {#mutdictpayloads}

#### Signature

```yql
MutDictPayloads(Linear<mutDictType for Dict<K,V>>)->Tuple<Linear<mutDictType for Dict<K,V>>,List<V>>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Gets a list of values ​​in a mutable dictionary and returns the same mutable dictionary and the result.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictPayloads($dict);
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},['1'])

SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        $dict, $val = MutDictPayloads($dict);
        return (FromMutDict($dict), $val);
    }); -- ({},[])
```

## MutDictItems {#mutdictitems}

#### Signature

```yql
MutDictItems(Linear<mutDictType for Dict<K,V>>)->Tuple<Linear<mutDictType for Dict<K,V>>,List<Tuple<K,V>>>
```

This function is available since version [2025.04](../changelog/2025.04.md).
Gets a list of tuples with key-value pairs in a mutable dictionary, returns the same mutable dictionary and the result.

#### Examples

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictItems($dict);
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},[('foo',1)])

SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        $dict, $val = MutDictItems($dict);
        return (FromMutDict($dict), $val);
    }); -- ({},[])
```
