
# Basic built-in functions

Below are the general-purpose functions. For specialized functions, there are separate articles: [aggregate functions](aggregation.md), [window functions](window.md), and functions for [lists](list.md), [dictionaries](dict.md), [structures](struct.md), [data types](types.md), and [code generation](codegen.md).



## COALESCE {#coalesce}

Iterates through the arguments from left to right and returns the first non-empty argument found. To be sure that the result is non-empty (not of an [optional type](../types/optional.md)), the rightmost argument must be of this type (often a literal is used for this). With a single argument, returns this argument unchanged.

Lets you pass potentially empty values to functions that can't handle them by themselves.

A short format using the low-priority `??` operator is available (lower than the Boolean operations). You can use the `NVL` alias.

#### Examples

```yql
SELECT COALESCE(
  maybe_empty_column,
  "it's empty!"
) FROM my_table;
```

```yql
SELECT
  maybe_empty_column ?? "it's empty!"
FROM my_table;
```

```yql
SELECT NVL(
  maybe_empty_column,
  "it's empty!"
) FROM my_table;
```

All three examples above are equivalent.



## LENGTH {#length}

Returns the length of the string in bytes. This function is also available under the `LEN` name .

#### Examples

```yql
SELECT LENGTH("foo");
```

```yql
SELECT LEN("bar");
```

{% note info %}

To calculate the length of a string in Unicode characters, you can use the function [Unicode::GetLength](../udf/list/unicode.md).<br/><br/>To get the number of elements in the list, use the function [ListLength](list.md#listlength).

{% endnote %}


## SUBSTRING {#substring}

Returns a substring.

Required arguments:

* Source string;
* Position: The offset from the beginning of the string in bytes (integer) or `NULL` meaning "from the beginning".

Optional arguments:

* Substring length: The number of bytes starting from the specified position (an integer, or the default `NULL` meaning "up to the end of the source string").

Indexing starts from zero. If the specified position and length are beyond the string, returns an empty string.
If the input string is optional, the result is also optional.

#### Examples

```yql
SELECT SUBSTRING("abcdefg", 3, 1); -- d
```

```yql
SELECT SUBSTRING("abcdefg", 3); -- defg
```

```yql
SELECT SUBSTRING("abcdefg", NULL, 3); -- abc
```



## FIND {#find}

Finding the position of a substring in a string.

Required arguments:

* Source string;
* The substring being searched for.

Optional arguments:

* A position in bytes to start the search with (an integer or `NULL` by default that means "from the beginning of the source string").

Returns the first substring position found or `NULL` (meaning that the desired substring hasn't been found starting from the specified position).

#### Examples

```yql
SELECT FIND("abcdefg_abcdefg", "abc"); -- 0
```

```yql
SELECT FIND("abcdefg_abcdefg", "abc", 1); -- 8
```

```yql
SELECT FIND("abcdefg_abcdefg", "abc", 9); -- null
```

## RFIND {#rfind}

Reverse finding the position of a substring in a string, from the end to the beginning.

Required arguments:

* Source string;
* The substring being searched for.

Optional arguments:

* A position in bytes to start the search with (an integer or `NULL` by default, meaning "from the end of the source string").

Returns the first substring position found or `NULL` (meaning that the desired substring hasn't been found starting from the specified position).

#### Examples

```yql
SELECT RFIND("abcdefg_abcdefg", "bcd"); -- 9
```

```yql
SELECT RFIND("abcdefg_abcdefg", "bcd", 8); -- 1
```

```yql
SELECT RFIND("abcdefg_abcdefg", "bcd", 0); -- null
```



## StartsWith, EndsWith {#starts_ends_with}

Checking for a prefix or suffix in a string.

Required arguments:

* Source string;
* The substring being searched for.

The arguments can be of the `String` or `Utf8` type and can be optional.

#### Examples

```yql
SELECT StartsWith("abc_efg", "abc") AND EndsWith("abc_efg", "efg"); -- true
```

```yql
SELECT StartsWith("abc_efg", "efg") OR EndsWith("abc_efg", "abc"); -- false
```

```yql
SELECT StartsWith("abcd", NULL); -- null
```

```yql
SELECT EndsWith(NULL, Utf8("")); -- null
```



## IF {#if}

Checks the condition: `IF(condition_expression, then_expression, else_expression)`.

It's a simplified alternative for [CASE WHEN ... THEN ... ELSE ... END](../syntax/expressions.md#case).

You may omit the `else_expression` argument. In this case, if the condition is false (`condition_expression` returned `false`), an empty value is returned with the type corresponding to `then_expression` and allowing for `NULL`. Hence, the result will have an [optional data type](../types/optional.md).

#### Examples

```yql
SELECT
  IF(foo > 0, bar, baz) AS bar_or_baz,
  IF(foo > 0, foo) AS only_positive_foo
FROM my_table;
```



## NANVL {#nanvl}

Replaces the values of `NaN` (not a number) in expressions like `Float`, `Double`, or [Optional](../types/optional.md).

Arguments:

1. The expression where you want to make a replacement.
2. The value to replace `NaN`.

If one of the arguments is `Double`, the result is`Double`, otherwise, it's `Float`. If one of the arguments is `Optional`, then the result is `Optional`.

#### Examples

```yql
SELECT
  NANVL(double_column, 0.0)
FROM my_table;
```



## Random... {#random}

Generates a pseudorandom number:

* `Random()`: A floating point number (Double) from 0 to 1.
* `RandomNumber()`: An integer from the complete Uint64 range.
* `RandomUuid()`: [Uuid version 4](https://tools.ietf.org/html/rfc4122#section-4.4).

#### Signatures

```yql
Random(T1[, T2, ...])->Double
RandomNumber(T1[, T2, ...])->Uint64
RandomUuid(T1[, T2, ...])->Uuid
```

No arguments are used for random number generation: they are only needed to control the time of the call. A new random number is returned at each call. Therefore:

* If Random is called again within a **same query** and with a same set of arguments, the same set of random numbers is returned. Keep in mind that we mean the arguments themselves (i.e., the text between parentheses) rather than their values.

* Calling of Random with the same set of arguments in **different queries** returns different sets of random numbers.

{% note warning %}

If Random is used in [named expressions](../syntax/expressions.md#named-nodes), its one-time calculation is not guaranteed. Depending on the optimizers and runtime environment, it can be counted both once and multiple times. To make sure it's only counted once, materialize a named expression into a table.

{% endnote %}

Use cases:

* `SELECT RANDOM(1);`: Get one random value for the entire query and use it multiple times (to get multiple random values, you can pass various constants of any type).
* `SELECT RANDOM(1) FROM table;`: The same random number for each row in the table.
* `SELECT RANDOM(1), RANDOM(2) FROM table;`: Two random numbers for each row of the table, all the numbers in each of the columns are the same.
* `SELECT RANDOM(some_column) FROM table;`: Different random numbers for each row in the table.
* `SELECT RANDOM(some_column), RANDOM(some_column) FROM table;`: Different random numbers for each row of the table, but two identical numbers within the same row.
* `SELECT RANDOM(some_column), RANDOM(some_column + 1) FROM table;` or `SELECT RANDOM(some_column), RANDOM(other_column) FROM table;`: Two columns, with different numbers in both.

#### Examples

```yql
SELECT
    Random(key) -- [0, 1)
FROM my_table;
```

```yql
SELECT
    RandomNumber(key) -- [0, Max<Uint64>)
FROM my_table;
```

```yql
SELECT
    RandomUuid(key) -- Uuid version 4
FROM my_table;
```

```yql
SELECT
    RANDOM(column) AS rand1,
    RANDOM(column) AS rand2, -- same as rand1
    RANDOM(column, 1) AS randAnd1, -- different from rand1/2
    RANDOM(column, 2) AS randAnd2 -- different from randAnd1
FROM my_table;
```

## Udf {#udf}

Builds a `Callable` given a function name and optional `external user types`, `RunConfig` and `TypeConfig`.

* `Udf(Foo::Bar)` — Function `Foo::Bar` without additional parameters.
* `Udf(Foo::Bar)(1, 2, 'abc')` — Call udf `Foo::Bar`.
* `Udf(Foo::Bar, Int32, @@{"device":"AHCI"}@@ as TypeConfig")(1, 2, 'abc')` — Call udf `Foo::Bar` with additional type `Int32` and specified `TypeConfig`.
* `Udf(Foo::Bar, "1e9+7" as RunConfig")(1, 'extended' As Precision)` — Call udf `Foo::Bar` with specified `RunConfig` and named parameters.
* `Udf(Foo::Bar, $parent as Depends)` — Call udf `Foo::Bar` with specified computation dependency on specified node - since version [2025.03](../changelog/2025.03.md).

#### Signatures

```yql
Udf(Callable[, T1, T2, ..., T_N][, V1 as TypeConfig][,V2 as RunConfig]])->Callable
```

Where `T1`, `T2`, etc. are additional (`external`) user types.

#### Examples

```yql
$IsoParser = Udf(DateTime2::ParseIso8601);
SELECT $IsoParser("2022-01-01");
```

```yql
SELECT Udf(Unicode::IsUtf)("2022-01-01")
```

```yql
$config = @@{ 
"name":"MessageFoo", 
"meta": "..."
}@@;
SELECT Udf(Protobuf::TryParse, $config As TypeConfig)("")
```

## CurrentUtc... {#current-utc}

`CurrentUtcDate()`, `CurrentUtcDatetime()` and `CurrentUtcTimestamp()`: Getting the current date and/or time in UTC. The result data type is specified at the end of the function name.

The arguments are optional and work same as [RANDOM](#random).

#### Examples

```yql
SELECT CurrentUtcDate();
```

```yql
SELECT CurrentUtcTimestamp(TableRow()) FROM my_table;
```



## CurrentTz... {#current-tz}

`CurrentTzDate()`, `CurrentTzDatetime()`, and `CurrentTzTimestamp()`: Get the current date and/or time in the [IANA time zone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) specified in the first argument. The result data type is specified at the end of the function name.

The arguments that follow are optional and work same as [RANDOM](#random).

#### Examples

```yql
SELECT CurrentTzDate("Europe/Moscow");
```

```yql
SELECT CurrentTzTimestamp("Europe/Moscow", TableRow()) FROM my_table;
```

## AddTimezone

Adding the time zone information to the date/time in UTC. In the result of `SELECT` or after `CAST`, a `String` will be subject to the time zone rules used to calculate the time offset.

Arguments:

1. Date: the type is `Date`/`Datetime`/`Timestamp`.
2. [The IANA name of the time zone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones).

Result type: `TzDate`/`TzDatetime`/`TzTimestamp`, depending on the input data type.

#### Examples

```yql
SELECT AddTimezone(Datetime("2018-02-01T12:00:00Z"), "Europe/Moscow");
```

## RemoveTimezone

Removing the time zone data and converting the value to date/time in UTC.

Arguments:

1. Date: the type is `TzDate`/`TzDatetime`/`TzTimestamp`.

Result type: `Date`/`Datetime`/`Timestamp`, depending on the input data type.

#### Examples

```yql
SELECT RemoveTimezone(TzDatetime("2018-02-01T12:00:00,Europe/Moscow"));
```



## Version {#version}

`Version()` returns a string describing the current version of the node processing the request. In some cases, such as during rolling upgrades, it might return different strings depending on which node processes the request. It does not accept any arguments.

#### Examples

```yql
SELECT Version();
```

## CurrentLanguageVersion {#current-language-version}

`CurrentLanguageVersion()` returns a string describing the current version of the language selected for the current request if it is defined, or an empty string.

#### Examples

```yql
SELECT CurrentLanguageVersion();
```


## MAX_OF, MIN_OF, GREATEST, and LEAST {#max-min}

Returns the minimum or maximum among N arguments. Those functions let you replace the SQL standard statement `CASE WHEN a < b THEN a ELSE b END` that would be too sophisticated for N more than two.

The argument types must be mutually castable and accept `NULL`.

`GREATEST` is a synonym for `MAX_OF` and `LEAST` is a synonym for `MIN_OF`.

#### Examples

```yql
SELECT MIN_OF(1, 2, 3);
```



## AsTuple, AsStruct, AsList, AsDict, AsSet, AsListStrict, AsDictStrict and AsSetStrict {#as-container}

Creates containers of the applicable types. For container literals, [operator notation](#containerliteral) is also supported.

Specifics:

* The container elements are passed in arguments. Hence, the number of elements in the resulting container is equal to the number of arguments passed, except when the dictionary keys repeat.
* `AsTuple` and `AsStruct` can be called without arguments, and also the arguments can have different types.
* The field names in `AsStruct` are set using `AsStruct(field_value AS field_name)`.
* Creating a list requires at least one argument if you need to output the element types. To create an empty list with the given type of elements, use the function [ListCreate](list.md#listcreate). You can create an empty list as an `AsList()` call without arguments. In this case, this expression will have the `EmptyList` type.
* Creating a dictionary requires at least one argument if you need to output the element types. To create an empty dictionary with the given type of elements, use the function [DictCreate](dict.md#dictcreate). You can create an empty dictionary as an `AsDict()` call without arguments, in this case, this expression will have the `EmptyDict` type.
* Creating a set requires at least one argument if you need to output element types. To create an empty set with the given type of elements, use the function [SetCreate](dict.md#setcreate). You can create an empty set as an `AsSet()` call without arguments, in this case, this expression will have the `EmptySet` type.
* `AsList` outputs the common type of elements in the list. A type error is raised in the case of incompatible types.
* `AsDict` separately outputs the common types for keys and values. A type error is raised in the case of incompatible types.
* `AsSet` outputs common types for keys. A type error is raised in the case of incompatible types.
* `AsListStrict`, `AsDictStrict`, `AsSetStrict` require the same type for their arguments.
* `AsDict` and `AsDictStrict` expect `Tuple` of two elements as arguments (key and value, respectively). If the keys repeat, only the value for the first key remains in the dictionary.
* `AsSet` and `AsSetStrict` expect keys as arguments.

#### Examples

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



## Container literals {#containerliteral}

Some containers support operator notation for their literal values:

* Tuple: `(value1, value2...)`;
* Structure: `<|name1: value1, name2: value2...|>`;
* List: `[value1, value2,...]`;
* Dictionary: `{key1: value1, key2: value2...}`;
* Set: `{key1, key2...}`.

In every case, you can use an insignificant trailing comma. For a tuple with one element, this comma is required: `(value1,)`.
For field names in the structure literal, you can use an expression that can be calculated at evaluation time, for example, string literals or identifiers (including those enclosed in backticks).

For nested lists, use [AsList](#as-container), for nested dictionaries, use [AsDict](#as-container), for nested sets, use [AsSet](#as-container), for nested tuples, use [AsTuple](#as-container), for nested structures, use [AsStruct](#as-container).

#### Examples

```yql
$name = "computed " || "member name";
SELECT
  (1, 2, "3") AS `tuple`,
  <|
    `complex member name`: 2.3,
    b: 2,
    $name: "3",
    "inline " || "computed member name": false
  |> AS `struct`,
  [1, 2, 3] AS `list`,
  {
    "a": 1,
    "b": 2,
    "c": 3,
  } AS `dict`,
  {1, 2, 3} AS `set`
```



## Variant {#variant}

`Variant()` creates a variant value over a tuple or structure.

Arguments:

* Value
* String with a field name or tuple index
* Variant type

#### Example

```yql
$var_type = Variant<foo: Int32, bar: Bool>;

SELECT
   Variant(6, "foo", $var_type) as Variant1Value,
   Variant(false, "bar", $var_type) as Variant2Value;
```

## AsVariant {#asvariant}

`AsVariant()` creates a value of a [variant over a structure](../types/containers.md) including one field. This value can be implicitly converted to any variant over a structure that has a matching data type for this field name and might include more fields with other names.

Arguments:

* Value
* A string with the field name

#### Example

```yql
SELECT
   AsVariant(6, "foo") as VariantValue
```

## Visit, VisitOrDefault {#visit}

Processes the possible values of a variant over a structure or tuple using the provided handler functions for each field/element of the variant.

#### Signature

```yql
Visit(Variant<key1: K1, key2: K2, ...>, K1->R AS key1, K2->R AS key2, ...)->R
Visit(Variant<K1, K2, ...>, K1->R, K2->R, ...)->R

VisitOrDefault(Variant<K1, K2, ...>{Flags:AutoMap}, R, [K1->R, [K2->R, ...]])->R
VisitOrDefault(Variant<key1: K1, key2: K2, ...>{Flags:AutoMap}, R, [K1->R AS key1, [K2->R AS key2, ...]])->R
```

### Arguments

* For a variant over structure: accepts the variant as the positional argument and named arguments (handlers) corresponding to each field of the variant.
* For a variant over tuple: accepts the variant and handlers for each element of the variant as positional arguments.
* `VisitOrDefault` includes an additional positional argument (on the second place) for the default value, enabling the omission of certain handlers.

#### Example

```yql
$vartype = Variant<num: Int32, flag: Bool, str: String>;
$handle_num = ($x) -> { return 2 * $x; };
$handle_flag = ($x) -> { return If($x, 200, 10); };
$handle_str = ($x) -> { return Unwrap(CAST(LENGTH($x) AS Int32)); };

$visitor = ($var) -> { return Visit($var, $handle_num AS num, $handle_flag AS flag, $handle_str AS str); };
SELECT
    $visitor(Variant(5, "num", $vartype)),                -- 10
    $visitor(Just(Variant(True, "flag", $vartype))),      -- Just(200)
    $visitor(Just(Variant("somestr", "str", $vartype))),  -- Just(7)
    $visitor(Nothing(OptionalType($vartype))),            -- Nothing(Optional<Int32>)
    $visitor(NULL)                                        -- NULL
;
```

## VariantItem {#variantitem}

Returns the value of a homogeneous variant (i.e., a variant containing fields/elements of the same type).

#### Signature

```yql
VariantItem(Variant<key1: K, key2: K, ...>{Flags:AutoMap})->K
VariantItem(Variant<K, K, ...>{Flags:AutoMap})->K
```

#### Example

```yql
$vartype1 = Variant<num1: Int32, num2: Int32, num3: Int32>;
SELECT
    VariantItem(Variant(7, "num2", $vartype1)),          -- 7
    VariantItem(Just(Variant(5, "num1", $vartype1))),    -- Just(5)
    VariantItem(Nothing(OptionalType($vartype1))),       -- Nothing(Optional<Int32>)
    VariantItem(NULL)                                    -- NULL
;
```

## Way {#way}

Returns an active field (active index) of a variant over a struct (tuple).

#### Signature

```yql
Way(Variant<key1: K1, key2: K2, ...>{Flags:AutoMap})->Utf8
Way(Variant<K1, K2, ...>{Flags:AutoMap})->Uint32
```

#### Example

```yql
$vr = Variant(1, "0", Variant<Int32, String>);
$vrs = Variant(1, "a", Variant<a:Int32, b:String>);
SELECT Way($vr);  -- 0
SELECT Way($vrs); -- "a"
```

## DynamicVariant {#dynamic_variant}

Creates a homogeneous variant instance (i.e. containing fields/elements of the same type), where the variant index or field can be set dynamically. If the index or field name does not exist, `NULL` will be returned.
The inverse function is [VariantItem](#variantitem).

#### Signature

```yql
DynamicVariant(item:T,index:Uint32?,Variant<T, T, ...>)->Optional<Variant<T, T, ...>>
DynamicVariant(item:T,index:Utf8?,Variant<key1: T, key2: T, ...>)->Optional<Variant<key1: T, key2: T, ...>>
```

#### Example

```yql
$dt = Int32;
$tvt = Variant<$dt,$dt>;
SELECT ListMap([(10,0u),(20,2u),(30,NULL)],($x)->(DynamicVariant($x.0,$x.1,$tvt))); -- [0: 10,NULL,NULL]

$dt = Int32;
$svt = Variant<x:$dt,y:$dt>;
SELECT ListMap([(10,'x'u),(20,'z'u),(30,NULL)],($x)->(DynamicVariant($x.0,$x.1,$svt))); -- [x: 10,NULL,NULL]

```

## Enum {#enum}

`Enum()` creates an enumeration value.

Arguments:

* A string with the field name
* Enumeration type

#### Example

```yql
$enum_type = Enum<Foo, Bar>;
SELECT
   Enum("Foo", $enum_type) as Enum1Value,
   Enum("Bar", $enum_type) as Enum2Value;
```

## AsEnum {#asenum}

`AsEnum()` creates a value of [enumeration](../types/containers.md) including one element. This value can be implicitly cast to any enumeration containing such a name.

Arguments:

* A string with the name of an enumeration item

#### Example

```yql
SELECT
   AsEnum("Foo");
```



## AsTagged, Untag {#as-tagged}

Wraps the value in the [Tagged data type](../types/special.md) with the specified tag, preserving the physical data type. `Untag`: The reverse operation.

Required arguments:

1. Value of any type.
2. Tag name.

Returns a copy of the value from the first argument with the specified tag in the data type.

Examples of use cases:

* Returns to the client's web interface the media files from BASE64-encoded strings.
* Prevent passing of invalid values at the boundaries of UDF calls.
* Additional refinements at the level of returned columns types.

## TablePath {#tablepath}

Access to the current table name, which might be needed when you use [CONCAT](../syntax/select/concat.md#concat), and other related functions.

No arguments. Returns a string with the full path or an empty string and warning when used in an unsupported context (for example, when working with a subquery or a range of 1000+ tables).

{% note info %}

The [TablePath](#tablepath), [TableName](#tablename), and [TableRecordIndex](#tablerecordindex) functions don't support temporary and anonymous tables (they return an empty string or 0 for [TableRecordIndex](#tablerecordindex)).
These functions are calculated when [executing](../syntax/select/index.md#selectexec) projections in `SELECT`, and by that time the current table may already be temporary.
To avoid such a situation, create a subquery for calculating these functions, as shown in the second example below.

{% endnote %}

#### Examples

```yql
SELECT TablePath() FROM CONCAT(table_a, table_b);
```

```yql
SELECT key, tpath_ AS path FROM (SELECT a.*, TablePath() AS tpath_ FROM RANGE(`my_folder`) AS a)
WHERE key IN $subquery;
```

## TableName {#tablename}

Get the table name based on the table path. You can obtain the path using the [TablePath](#tablepath) function.

Optional arguments:

* Path to the table, `TablePath()` is used by default (see also its limitations).
* Specifying the system ("yt") whose rules are used to determine the table name. You need to specify the system only if [USE](../syntax/use.md) doesn't specify the current cluster.

#### Examples

```yql
USE cluster;
SELECT TableName() FROM CONCAT(table_a, table_b);
```


## TableRecordIndex {#tablerecordindex}

Access to the current sequence number of a row in the physical source table, **starting from 1** (depends on the storage implementation).

No arguments. When used in combination with [CONCAT](../syntax/select/concat.md#concat), and other similar mechanisms, numbering restarts for each input table. If used in an incorrect context, it returns 0.

#### Example

```yql
SELECT TableRecordIndex() FROM my_table;
```

## TableRow, JoinTableRow {#tablerow}

Getting the entire table row as a structure. No arguments. `JoinTableRow` in case of `JOIN` always returns a structure with table prefixes.

#### Example

```yql
SELECT TableRow() FROM my_table;
```

## FileContent and FilePath {#file-content-path}

The `FileContent` and `FilePath` argument is a string with an alias.

#### Examples

```yql
SELECT "Content of "
  || FilePath("my_file.txt")
  || ":\n"
  || FileContent("my_file.txt");
```

## FolderPath {#folderpath}

Getting the path to the root of a directory with several "attached" files with the common prefix specified.

The argument is a string with a prefix among aliases.

See also [PRAGMA File](../syntax/pragma/file.md#file) and [PRAGMA Folder](../syntax/pragma/file.md#folder).

#### Examples

```yql
PRAGMA File("foo/1.txt", "http://url/to/somewhere");
PRAGMA File("foo/2.txt", "http://url/to/somewhere/else");
PRAGMA File("bar/3.txt", "http://url/to/some/other/place");

SELECT FolderPath("foo"); -- The directory at the return path will
                          -- include the files 1.txt and 2.txt downloaded from the above links
```

## ParseFile

Get a list of values from the attached text file. It can be combined with [IN](../syntax/expressions.md#in), attaching the file by URL.

Only one file format is supported: one value per line.

Two required arguments:

1. List cell type: only strings and numeric types are supported.
2. The name of the attached file.

{% note info %}

The return value is a lazy list. For repeat use, wrap it in the function [ListCollect](list.md#listcollect)

{% endnote %}

#### Examples

```yql
SELECT ListLength(ParseFile("String", "my_file.txt"));
```

```yql
SELECT * FROM my_table
WHERE int_column IN ParseFile("Int64", "my_file.txt");
```

## Ensure... {#ensure}

Checking for the user conditions:

* `Ensure()`: Checking whether the predicate is true at query execution.
* `EnsureType()`: Checking that the expression type exactly matches the specified type.
* `EnsureConvertibleTo()`: A soft check of the expression type (with the same rules as for implicit type conversion).

If the check fails, the entire query fails.

Arguments:

1. An expression that will result from a function call if the check is successful. It's also checked for the data type in the corresponding functions.
2. Ensure uses a Boolean predicate that is checked for being `true`. The other functions use the data type that can be obtained using the [relevant functions](types.md), or a string literal with a [text description of the type](../types/type_string.md).
3. An optional string with an error comment to be included in the overall error message when the query is complete. The data itself can't be used for type checks, since the data check is performed at query validation (or can be an arbitrary expression in the case of Ensure).

To check the conditions based on the final calculation result, it's convenient to combine Ensure with [DISCARD SELECT](../syntax/discard.md).

#### Examples

```yql
SELECT Ensure(
    value,
    value < 100,
    "value out or range"
) AS value FROM my_table;
```

```yql
SELECT EnsureType(
    value,
    TypeOf(other_value),
    "expected value and other_value to be of same type"
) AS value FROM my_table;
```

```yql
SELECT EnsureConvertibleTo(
    value,
    Double?,
    "expected value to be numeric"
) AS value FROM my_table;
```

## EvaluateExpr, EvaluateAtom {#evaluate_expr_atom}

Evaluate an expression before the start of the main calculation and input its result to the query as a literal (constant). In many contexts, where only a constant would be expected in standard SQL (for example, in table names, in the number of rows in [LIMIT](../syntax/select/limit_offset.md), and so on), this functionality is implicitly enabled automatically.

EvaluateExpr can be used where the grammar already expects an expression. For example, you can use it to:

* Round the current time to days, weeks, or months and insert it into the query to ensure correct query caching, although usually when [functions are used to get the current time](#current-utc), query caching is completely disabled.
* Run a heavy calculation with a small result once per query instead of once per job.

EvaluateAtom lets you dynamically create an [atom](../types/special.md), but since atoms are mainly controlled from a lower [s-expressions](/docs/s_expressions/functions) level, it's generally not recommended to use this function directly.

The only argument for both functions is the expression for calculation and substitution.

Restrictions:

* The expression must not trigger MapReduce operations.
* This functionality is fully locked in YQL over YDB.

#### Examples

```yql
$now = CurrentUtcDate();
SELECT EvaluateExpr(
    DateTime::MakeDate(DateTime::StartOfWeek($now)
    )
);
```


## Literals of primitive types {#data-type-literals}

For primitive types, you can create literals based on string literals.

#### Syntax

`<Primitive type>( <string>[, <additional attributes>] )`

Unlike `CAST("myString" AS MyType)`:

* The check for literal's castability to the desired type occurs at validation.
* The result is non-optional.

For the data types `Date`, `Datetime`, `Timestamp`, and `Interval`, literals are supported only in the format corresponding to [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601). `Interval` has the following differences from the standard:

* It supports the negative sign for shifts to the past.
* Microseconds can be expressed as fractional parts of seconds.
* You can't use units of measurement exceeding one week.
* The options with the beginning/end of the interval and with repetitions, are not supported.

For the data types `TzDate`, `TzDatetime`, `TzTimestamp`, literals are also set in the format meeting [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), but instead of the optional Z suffix, they specify the [IANA name of the time zone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones), separated by comma (for example, GMT or Europe/Moscow).

{% include [decimal args](../_includes/decimal_args.md) %}

#### Examples

```yql
SELECT
  Bool("true"),
  Uint8("0"),
  Int32("-1"),
  Uint32("2"),
  Int64("-3"),
  Uint64("4"),
  Float("-5"),
  Double("6"),
  Decimal("1.23", 5, 2), -- up to 5 decimal digits, with 2 after the decimal point
  String("foo"),
  Utf8("Hello"),
  Yson("<a=1>[3;%false]"),
  Json(@@{"a":1,"b":null}@@),
  Date("2017-11-27"),
  Datetime("2017-11-27T13:24:00Z"),
  Timestamp("2017-11-27T13:24:00.123456Z"),
  Interval("P1DT2H3M4.567890S"),
  TzDate("2017-11-27,Europe/Moscow"),
  TzDatetime("2017-11-27T13:24:00,America/Los_Angeles"),
  TzTimestamp("2017-11-27T13:24:00.123456,GMT"),
  Uuid("f9d5cc3f-f1dc-4d9c-b97e-766e57ca4ccb");
```

## ToBytes and FromBytes {#to-from-bytes}

Conversion of [primitive data types](../types/primitive.md) to a string with their binary representation and back. Numbers are represented in the [little endian](https://en.wikipedia.org/wiki/Endianness#Little-endian) format.

#### Examples

```yql
SELECT
    ToBytes(123), -- "\u0001\u0000\u0000\u0000"
    FromBytes(
        "\xd2\x02\x96\x49\x00\x00\x00\x00",
        Uint64
    ); -- 1234567890ul
```



## ByteAt {#byteat}

Getting the byte value inside a string at an index counted from the beginning of the string. If an invalid index is specified, `NULL` is returned.

Arguments:

1. String: `String` or `Utf8`.
2. Index: `Uint32`.

#### Examples

```yql
SELECT
    ByteAt("foo", 0), -- 102
    ByteAt("foo", 1), -- 111
    ByteAt("foo", 9); -- NULL
```



## ...Bit {#bitops}

`TestBit()`, `ClearBit()`, `SetBit()` and `FlipBit()`: Test, clear, set, or flip a bit in an unsigned number using the specified bit sequence number.

Arguments:

1. An unsigned number that's subject to the operation. `TestBit` is also implemented for strings (see the description below).
2. Number of the bit.

`TestBit` returns `true/false`. The other functions return a copy of their first argument with the corresponding conversion.

`TestBit` works the following way for the string argument:

1. For the second argument (the number of the bit) the corresponding byte **from the beginning of the string** is chosen.
2. Next, for the given byte the corresponding LSB is chosen.

#### Examples

```yql
SELECT
    TestBit(1u, 0), -- true
    TestBit('ax', 12) -- true (second byte, fourth bit)
    SetBit(8u, 0); -- 9
```



## Abs {#abs}

The absolute value of the number.

#### Examples

```yql
SELECT Abs(-123); -- 123
```



## Just {#optional-ops}

`Just()`: Change the value's data type to [optional](../types/optional.md) from the current data type (i.e.,`T` is converted to `T?`).

The reverse operation is [Unwrap](#optional-ops).

#### Examples

```yql
SELECT
  Just("my_string"); --  String?
```

## Unwrap {#unwrap}

`Unwrap()`: Converting the [optional](../types/optional.md) value of the data type to the relevant non-optional type, raising a runtime error if the data is `NULL`. This means that `T?` becomes `T`.

If the value isn't [optional](../types/optional.md), then the function returns its first argument unchanged.

Arguments:

1. Value to be converted.
2. An optional string with a comment for the error text.

Reverse operation is [Just](#optional-ops).

#### Examples

```yql
$value = Just("value");

SELECT Unwrap($value, "Unexpected NULL for $value");
```

## Nothing {#nothing}

`Nothing()`: Create an empty value for the specified [Optional](../types/optional.md) data type.

#### Examples

```yql
SELECT
  Nothing(String?); -- an empty (NULL) value with the String? type
```

[Learn more about ParseType and other functions for data types](types.md).



## Callable {#callable}

Create a callable value with the specified signature from a lambda function. It's usually used to put callable values into containers.

Arguments:

1. Type.
2. Lambda function.

#### Examples

```yql
$lambda = ($x) -> {
    RETURN CAST($x as String)
};

$callables = AsTuple(
    Callable(Callable<(Int32)->String>, $lambda),
    Callable(Callable<(Bool)->String>, $lambda),
);

SELECT $callables.0(10), $callables.1(true);
```



## Pickle, Unpickle {#pickle}

`Pickle()` and `StablePickle()` serialize an arbitrary object into a sequence of bytes, if possible. Typical non-serializable objects are Callable and Resource. The serialization format is not versioned and can be used within a single query. For the Dict type, the StablePickle function pre-sorts the keys, and for Pickle, the order of dictionary elements in the serialized representation isn't defined.

`Unpickle()` is the inverse operation (deserialization), where with the first argument being the data type of the result and the second argument is the string with the result of `Pickle()` or `StablePickle()`.

#### Examples

```yql
SELECT *
FROM my_table
WHERE Digest::MurMurHash32(
        Pickle(TableRow())
    ) %10 ==0; -- actually, it is better to use TABLESAMPLE

$buf = Pickle(123);
SELECT Unpickle(Int32, $buf);
```



## StaticMap

Transforms a structure or tuple by applying a lambda function to each item.

Arguments:

* Structure or tuple.
* Lambda for processing items.

Result: a structure or tuple with the same number and naming of items as in the first argument, and with item data types determined by lambda results.

#### Examples

```yql
SELECT *
FROM (
    SELECT
        StaticMap(TableRow(), ($item) -> {
            return CAST($item AS String);
        })
    FROM my_table
) FLATTEN COLUMNS; -- converting all columns to rows
```



## StaticZip

Merges structures or tuples element-by-element. All arguments (one or more) must be either structures with the same set of fields or tuples of the same length.
The result will be a structure or tuple, respectively.
Each item of the result is a tuple comprised of items taken from arguments.

#### Examples

```yql
$one = <|k1:1, k2:2.0|>;
$two = <|k1:3.0, k2:4|>;

-- Adding two structures item-by-item
SELECT StaticMap(StaticZip($one, $two), ($tuple)->($tuple.0 + $tuple.1)) AS sum;
```



## StaticFold, StaticFold1 {#staticfold}

```yql
StaticFold(obj:Struct/Tuple, initVal, updateLambda)
StaticFold1(obj:Struct/Tuple, initLambda, updateLambda)
```

Left fold over struct/tuple elements.
The folding of tuples is done in order from the element with the lower index to the element with the larger one; for structures, the order is not guaranteed.

- `obj` - object to fold
- `initVal` - _(for StaticFold)_ initial fold state
- `initLambda` - _(for StaticFold1)_ lambda that produces initial fold state from the first element
- `updateLambda` - lambda that produces the new state (arguments are the next element and the previous state)


`StaticFold(<|key_1:$el_1, key_2:$el_2, ..., key_n:$el_n|>, $init, $f)` transforms into:

```yql
$f($el_n, ...$f($el_2, $f($init, el_1))...)
```

`StaticFold1(<|key_1:$el_1, key_2:$el_2, ..., key_n:$el_n|>, $f0, $f)`:

```yql
$f($el_n, ...$f($el_2, $f($f0($init), el_1))...)
```

`StaticFold1(<||>, $f0, $f)` returns `NULL`.

Works with tuples in the same way.


## AggregationFactory {#aggregationfactory}

Create a factory for [aggregation functions](aggregation.md) to separately describe the methods of aggregation and data types subject to aggregation.

Arguments:

1. A string in double quotes with the name of an aggregate function, for example ["MIN"](aggregation.md#min).
2. Optional parameters of the aggregate function that are data-independent. For example, the percentile value in [PERCENTILE](aggregation.md#percentile).

The resulting factory can be used as the second parameter of the function [AGGREGATE_BY](aggregation.md#aggregate-by).
If the aggregate function is applied to two columns instead of one, as, for example, [MIN_BY](aggregation.md#minby), then in [AGGREGATE_BY](aggregation.md#aggregate-by), the first argument passes a `Tuple` of two values. See more details in the description of the applicable aggregate function.

#### Examples

```yql
$factory = AggregationFactory("MIN");
SELECT
    AGGREGATE_BY (value, $factory) AS min_value -- apply the MIN aggregation to the "value" column
FROM my_table;
```

## AggregateTransformInput {#aggregatetransform}

`AggregateTransformInput()` converts an [aggregation factory](aggregation.md), for example, obtained using the [AggregationFactory](#aggregationfactory) function, to other factory, in which the specified transformation of input items is performed before starting aggregation.

Arguments:

1. Aggregation factory.
2. A lambda function with one argument that converts an input item.

#### Examples

```yql
$f = AggregationFactory("sum");
$g = AggregateTransformInput($f, ($x) -> (cast($x as Int32)));
$h = AggregateTransformInput($f, ($x) -> ($x * 2));
SELECT ListAggregate([1,2,3], $f); -- 6
SELECT ListAggregate(["1","2","3"], $g); -- 6
SELECT ListAggregate([1,2,3], $h); -- 12
```

## AggregateTransformOutput {#aggregatetransformoutput}

`AggregateTransformOutput()` converts an [aggregation factory](aggregation.md), for example, obtained using the [AggregationFactory](#aggregationfactory) function, to other factory, in which the specified transformation of the result is performed after ending aggregation.

Arguments:

1. Aggregation factory.
2. A lambda function with one argument that converts the result.

#### Examples

```yql
$f = AggregationFactory("sum");
$g = AggregateTransformOutput($f, ($x) -> ($x * 2));
SELECT ListAggregate([1,2,3], $f); -- 6
SELECT ListAggregate([1,2,3], $g); -- 12
```

## AggregateFlatten {#aggregateflatten}

Adapts a factory for [aggregation functions](aggregation.md), for example, obtained using the [AggregationFactory](#aggregationfactory) function in a way that allows aggregation of list input items. This operation is similar to [FLATTEN LIST BY](../syntax/flatten.md): Each list item is aggregated.

Arguments:

1. Aggregation factory.

#### Examples

```yql
$i = AggregationFactory("AGGREGATE_LIST_DISTINCT");
$j = AggregateFlatten($i);
SELECT AggregateBy(x, $j) from (
   select [1,2] as x
   union all
   select [2,3] as x
); -- [1, 2, 3]
```

## YQL::, s-expressions {#s-expressions}

For the full list of internal YQL functions, see the [documentation for s-expressions](/docs/s_expressions/functions), an alternative low-level YQL syntax. Any of the functions listed there can also be called from the SQL syntax by adding the `YQL::` prefix to its name. However, we don't recommend doing this, because this mechanism is primarily intended to temporarily bypass possible issues and for internal testing purposes.

If the function is available in SQL syntax without the `YQL::` prefix, then its behavior may differ from the same-name function from the s-expressions documentation, if any.

