## AsTuple, AsStruct, AsList, AsDict, AsSet, AsListStrict, AsDictStrict and AsSetStrict {#as-container}

Creates containers of the applicable types. For container literals, [operator notation](#containerliteral) is also supported.

Specifics:

* The container elements are passed in arguments. Hence, the number of elements in the resulting container is equal to the number of arguments passed, except when the dictionary keys repeat.
* `AsTuple` and `AsStruct` can be called without arguments, and also the arguments can have different types.
* The field names in `AsStruct` are set using `AsStruct(field_value AS field_name)`.
* Creating a list requires at least one argument if you need to output the element types. To create an empty list with the given type of elements, use the function [ListCreate](../../list.md#listcreate). You can create an empty list as an `AsList()` call without arguments. In this case, this expression will have the `EmptyList` type.
* Creating a dictionary requires at least one argument if you need to output the element types. To create an empty dictionary with the given type of elements, use the function [DictCreate](../../dict.md#dictcreate). You can create an empty dictionary as an `AsDict()` call without arguments, in this case, this expression will have the `EmptyDict` type.
* Creating a set requires at least one argument if you need to output element types. To create an empty set with the given type of elements, use the function [SetCreate](../../dict.md#setcreate). You can create an empty set as an `AsSet()` call without arguments, in this case, this expression will have the `EmptySet` type.
* `AsList` outputs the common type of elements in the list. A type error is raised in the case of incompatible types.
* `AsDict` separately outputs the common types for keys and values. A type error is raised in the case of incompatible types.
* `AsSet` outputs common types for keys. A type error is raised in the case of incompatible types.
* `AsListStrict`, `AsDictStrict`, `AsSetStrict` require the same type for their arguments.
* `AsDict` and `AsDictStrict` expect `Tuple` of two elements as arguments (key and value, respectively). If the keys repeat, only the value for the first key remains in the dictionary.
* `AsSet` and `AsSetStrict` expect keys as arguments.

**Examples**

```yql
SELECT
  AsTuple(1, 2, "3") AS `tuple`,
  AsStruct(
    1 AS a,
    2 AS b,
    "3" AS c
  ) AS `struct`,
  AsList(1, 2, 3) AS `list`,
  AsDict(
    AsTuple("a", 1),
    AsTuple("b", 2),
    AsTuple("c", 3)
  ) AS `dict`,
  AsSet(1, 2, 3) AS `set`
```

