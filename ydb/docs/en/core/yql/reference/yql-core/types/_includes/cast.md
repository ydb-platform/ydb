# Rules for type casting using the operator [CAST](../../syntax/expressions.md#cast)

## Rules for casting primitive data types.

* When casting primitive data types, some of the source information may be discarded unless contained in the target type. For example:

  * The `Float`/`Double` fractional part, when casting to integer types.
  * The `Datetime`/`Timestamp` time, when casting to `Date`.
  * The timezone, when casting from timezone types to date/time types without a timezone.

* If, in a certain combination of the source and target type, casting can't be performed for all possible values of the source type, then, if the casting fails, `CAST` returns `NULL`. In such cases, one `Optional` level is added to the return value type, unless already present. For example, the constructs: `CAST("3.14" AS Float?)` and `CAST("3.14" AS Float)` are fully equivalent and return `Float?`.
* If casting is possible for all values of the source type, then adding '?' works the same way as`Just` on top: `CAST(3.14 AS Utf8?)` is same as `Just(CAST(3.14 AS Utf8))`
All combinations of primitive data types for which `CAST` can be used are described [here](../primitive.md).

## Casting rules for containers.

### Rules for Optional

* If a higher `Optional` level is set for the target type than for the source type, it's same as adding `Just` on top of `CAST` with a lower `Optional` level.
* If the source type has a higher level of `Optional` for the source type, then `NULL` at any level higher than the target level results in `NULL`.
* At equal levels of `Optional`, the `NULL` value preserves the same level.

```yql
SELECT
    CAST(1 AS Int32?),                  -- is equivalent to Just(1)
    CAST(Just(2/1) AS Float??),         -- [2]
    CAST(Just(3/0) AS Float??) IS NULL; -- false: the result is Just(NULL)
```

### Rules for List/Dict

* To create a list, `CAST` is applied to each item in the source list to cast it to the target type.
* If the target item type is non-optional and `CAST` on the item might fail, then such casting is discarded. In this case, the resulting list might be shorter or even empty if every casting failed.
* For dictionaries, the casting is totally similar to lists, with `CAST` being applied to keys and values.

```yql
SELECT
    CAST([-1, 0, 1] AS List<Uint8?>),             -- [null, 0, 1]
    CAST(["3.14", "bad", "42"] AS List<Float>),   -- [3.14, 42]

    CAST({-1:3.14, 7:1.6} AS Dict<Uint8, Utf8>),  -- {7: "1.6"}
    CAST({-1:3.14, 7:1.6} AS Dict<Uint8?, Utf8>); -- {7: "1.6", null:"3.14"}
```

### Rules for Struct/Tuple

* A structure or tuple is created by applying `CAST` to each item of the source type to cast it to an item with the same name or target type index.
* If some field is missing in the target type, it's simply discarded.
* If some field is missing in the source value type, then it can be added only if it's optional and accepts the `NULL` value.
* If some field is non-optional in the target type, but its casting might fail, then `CAST` adds Optional to the structure or tuple level and might return `NULL` for the entire result.

```yql
SELECT
    CAST((-1, 0, 1) AS Tuple<Uint16?, Uint16?, Utf8>), -- (null, 0, "1")
    CAST((-2, 0) AS Tuple<Uint16, Utf8>),              -- null
    CAST((3, 4) AS Tuple<Uint16, String>),             -- (3, "4"): the type is Tuple<Uint16, String>?
    CAST(("4",) AS Tuple<Uint16, String?>),            -- (4, null)
    CAST((5, 6, null) AS Tuple<Uint8?>);               -- (5,): the items were removed.

SELECT -- One field was removed and one field was added: ("three":null, "two": "42")
    CAST(<|one:"8912", two:42|> AS Struct<two:Utf8, three:Date?>);
```

### Rules for Variant

* A variant with a specific name or index is cast to a variant with the same name or index.
* If casting of a variant might fail and the type of this variant is non-optional, then `CAST` adds Optional to the top level and can return `NULL`.
* If some variant is missing in the target type, then `CAST` adds Optional to the top level and returns `NULL` for such a value.

### Nested containers

* All of the above rules are applied recursively for nested containers.

