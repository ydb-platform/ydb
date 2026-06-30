# Basic built-in functions

Below are general-purpose functions, while specialized functions have separate articles: [aggregate](aggregation.md){% if feature_window_functions %}, [window](window.md){% endif %}, as well as for working with [lists](list.md), [dictionaries](dict.md), [structures](struct.md), [data types](types.md){% if feature_codegen %} and [code generation](codegen.md){% endif %}.

## COALESCE {#coalesce}

Iterates over arguments from left to right and returns the first non-empty argument found. To ensure the result is non-empty (not an [optional type](../types/optional.md)), the rightmost argument must be of that type (often a literal is used). With a single argument, returns it unchanged.

### Signature


```yql
COALESCE(T?, ..., T)->T
COALESCE(T?, ..., T?)->T?
```


Allows passing potentially empty values to functions that cannot handle them on their own.

A short form is available as the `??` operator. You can use the alias `NVL`.

### Examples


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

Returns the length of a string in bytes. This function is also available under the name `LEN`.

### Signature


```yql
LENGTH(T)->Uint32
LENGTH(T?)->Uint32?
```


### Examples


```yql
SELECT LENGTH("foo");
```


```yql
SELECT LEN("bar");
```


{% note info %}

To calculate the length of a string in Unicode characters, use the [Unicode::GetLength](../udf/list/unicode.md) function.<br/><br/>To get the number of elements in a list, use the [ListLength](list.md#listlength) function.

{% endnote %}

## SUBSTRING {#substring}

Returns a substring.

### Signature


```yql
Substring(String[, Uint32? [, Uint32?]])->String
Substring(String?[, Uint32? [, Uint32?]])->String?
```


Required arguments:

* Source string.
* Position — offset from the beginning of the string in bytes (integer) or `NULL`, meaning 'from the beginning'.

Optional arguments:

* Substring length — number of bytes starting from the specified position (integer, or `NULL` by default, meaning 'to the end of the source string').

Indexing starts from zero. If the specified position and length exceed the string boundaries, an empty string is returned.
If the input string is optional, the result is also optional.

### Examples


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

Search for a substring position in a string.

### Signature


```yql
Find(String, String[, Uint32?])->Uint32?
Find(String?, String[, Uint32?])->Uint32?
Find(Utf8, Utf8[, Uint32?])->Uint32?
Find(Utf8?, Utf8[, Uint32?])->Uint32?
```


Required arguments:

* Source string.
* Target substring.

Optional arguments:

* Position — in bytes, from which to start the search (integer, or `NULL` by default, meaning 'from the beginning of the source string').

Returns the first found position of the substring, or `NULL`, meaning that the target substring was not found from the specified position.

### Examples


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

Reverse search for a substring position in a string, from end to beginning.

### Signature


```yql
RFind(String, String[, Uint32?])->Uint32?
RFind(String?, String[, Uint32?])->Uint32?
RFind(Utf8, Utf8[, Uint32?])->Uint32?
RFind(Utf8?, Utf8[, Uint32?])->Uint32?
```


Required arguments:

* Source string.
* Target substring.

Optional arguments:

* Position — in bytes, from which to start the search (integer, or `NULL` by default, meaning 'from the end of the source string').

Returns the first found position of the substring, or `NULL`, meaning that the target substring was not found from the specified position.

### Examples


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

Checks for the presence of a prefix or suffix in a string.

### Signatures


```yql
StartsWith(T str, U prefix)->Bool[?]

EndsWith(T str, U suffix)->Bool[?]
```


Required arguments:

* Source string.
* Target substring.

Arguments must be of type `String`/`Utf8` (or optional `String`/`Utf8`) or a PostgreSQL string type (`PgText`/`PgBytea`/`PgVarchar`).
The function result is an optional Bool, except when both arguments are non-optional – in that case, Bool is returned.

### Examples


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


```yql
SELECT StartsWith("abc_efg"u, "abc"p) AND EndsWith("abc_efg", "efg"pv); -- true
```


## IF {#if}

Checks the condition `IF(condition_expression, then_expression, else_expression)`.

Is a simplified alternative to [CASE WHEN ... THEN ... ELSE ... END](../syntax/expressions.md#case).

### Signature


```yql
IF(Bool, T, T)->T
IF(Bool, T)->T?
```


The `else_expression` argument can be omitted. In this case, if the condition is false (`condition_expression` returned `false`), an empty value will be returned with a type corresponding to `then_expression` and allowing the value `NULL`. Thus, the result will have an [optional data type](../types/optional.md).

### Examples


```yql
SELECT
  IF(foo > 0, bar, baz) AS bar_or_baz,
  IF(foo > 0, foo) AS only_positive_foo
FROM my_table;
```


## NANVL {#nanvl}

Replaces `NaN` (not a number) values in expressions of type `Float`, `Double`, or [Optional](../types/optional.md).

### Signature


```yql
NANVL(Float, Float)->Float
NANVL(Double, Double)->Double
```


Arguments:

1. Expression in which to perform the replacement.
2. Value to replace `NaN` with.

If one of the arguments is `Double`, then the output is `Double`, otherwise `Float`. If one of the arguments is `Optional`, then the output is also `Optional`.

### Examples


```yql
SELECT
  NANVL(double_column, 0.0)
FROM my_table;
```


## Random... {#random}

Generates a pseudo-random number:

* `Random()` — floating-point number (Double) from 0 to 1.
* `RandomNumber()` — integer from the entire Uint64 range.
* `RandomUuid()` — [Uuid version 4](https://tools.ietf.org/html/rfc4122#section-4.4).

### Signatures


```yql
Random(T1[, T2, ...])->Double
RandomNumber(T1[, T2, ...])->Uint64
RandomUuid(T1[, T2, ...])->Uuid
```


When generating random numbers, the arguments are not used and are needed solely to control the moment of the call. At each call moment, a new random number is returned. Therefore:

{% if ydb_non_deterministic_functions %}

* Repeated calls to Random within **a single query** with an identical set of arguments do not guarantee obtaining the same sets of random numbers. The values will be equal if the Random calls fall into the same execution phase.

{% else %}

* Repeated calls to Random within **a single query** with an identical set of arguments return the same set of random numbers. It is important to understand that this refers to the arguments themselves (the text between parentheses), not their values.

{% endif %}

* Calls to Random with the same set of arguments in **different queries** will return different sets of random numbers.

{% note warning %}

If Random is used in [named expressions](../syntax/expressions.md#named-nodes), its single evaluation is not guaranteed. Depending on the optimizers and execution environment, it may be computed once or multiple times. To guarantee a single computation, you must materialize the named expression into a table in this case.

{% endnote %}

Use cases:

* `SELECT RANDOM(1);` — get one random value for the entire query and use it multiple times (to get multiple values, you can pass different constants of any type).
* `SELECT RANDOM(1) FROM table;` — the same random number for each row of the table.
* `SELECT RANDOM(1), RANDOM(2) FROM table;` — two random numbers for each row of the table, all numbers in each column are the same.
* `SELECT RANDOM(some_column) FROM table;` — different random numbers for each row of the table.
* `SELECT RANDOM(some_column), RANDOM(some_column) FROM table;` — different random numbers for each row of the table, but within a single row — two identical numbers.
* `SELECT RANDOM(some_column), RANDOM(some_column + 1) FROM table;` or `SELECT RANDOM(some_column), RANDOM(other_column) FROM table;` — two columns, all with different numbers.

### Examples


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


## UDF {#udf}

Constructs `Callable` by the given function name and optional `external user types`, `RunConfig`, and `TypeConfig`.

* `Udf(Foo::Bar)` — Function `Foo::Bar` without additional parameters.
* `Udf(Foo::Bar)(1, 2, 'abc')` — Call UDF `Foo::Bar`.
* `Udf(Foo::Bar, Int32, @@{"device":"AHCI"}@@ as TypeConfig")(1, 2, 'abc')` — Call UDF `Foo::Bar` with additional type `Int32` and specified `TypeConfig`.
* `Udf(Foo::Bar, "1e9+7" as RunConfig")(1, 'extended' As Precision)` — Call UDF `Foo::Bar` with specified `RunConfig` and named parameters.

### Signatures


```yql
Udf(Callable[, T1, T2, ..., T_N][, V1 as TypeConfig][,V2 as RunConfig]])->Callable
```


Where `T1`, `T2`, etc. are additional (`external`) user-defined types.

### Examples


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

`CurrentUtcDate()`, `CurrentUtcDatetime()`, and `CurrentUtcTimestamp()` — getting the current date and/or time in UTC. The result data type is indicated at the end of the function name.

### Signatures


```yql
CurrentUtcDate(...)->Date
CurrentUtcDatetime(...)->Datetime
CurrentUtcTimestamp(...)->Timestamp
```


Arguments are optional and work on the same principle as [RANDOM](#random).

### Examples


```yql
SELECT CurrentUtcDate();
```


```yql
SELECT CurrentUtcTimestamp(TableRow()) FROM my_table;
```


## CurrentTz... {#current-tz}

`CurrentTzDate()`, `CurrentTzDatetime()`, and `CurrentTzTimestamp()` — getting the current date and/or time in the [IANA time zone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) specified in the first argument. The result data type is indicated at the end of the function name.

### Signatures


```yql
CurrentTzDate(String, ...)->TzDate
CurrentTzDatetime(String, ...)->TzDatetime
CurrentTzTimestamp(String, ...)->TzTimestamp
```


Subsequent arguments are optional and work on the same principle as [RANDOM](#random).

### Examples


```yql
SELECT CurrentTzDate("Europe/Moscow");
```


```yql
SELECT CurrentTzTimestamp("Europe/Moscow", TableRow()) FROM my_table;
```


## AddTimezone

Adding time zone information to a date/time specified in UTC. When output in `SELECT` or after `CAST` in `String`, the time zone rules for calculating the time offset will be applied.

### Signature


```yql
AddTimezone(Date, String)->TzDate
AddTimezone(Date?, String)->TzDate?
AddTimezone(Datetime, String)->TzDatetime
AddTimezone(Datetime?, String)->TzDatetime?
AddTimezone(Timestamp, String)->TzTimestamp
AddTimezone(Timestamp?, String)->TzTimestamp?
```


Arguments:

1. Date — type `Date`/`Datetime`/`Timestamp`.
2. [IANA time zone name](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones).

Result type — `TzDate`/`TzDatetime`/`TzTimestamp`, depending on the input data type.

### Examples


```yql
SELECT AddTimezone(Datetime("2018-02-01T12:00:00Z"), "Europe/Moscow");
```


## RemoveTimezone

Removing time zone information and converting to date/time specified in UTC.

### Signature


```yql
RemoveTimezone(TzDate)->Date
RemoveTimezone(TzDate?)->Date?
RemoveTimezone(TzDatetime)->Datetime
RemoveTimezone(TzDatetime?)->Datetime?
RemoveTimezone(TzTimestamp)->Timestamp
RemoveTimezone(TzTimestamp?)->Timestamp?
```


Arguments:

1. Date — type `TzDate`/`TzDatetime`/`TzTimestamp`.

Result type — `Date`/`Datetime`/`Timestamp`, depending on the input data type.

### Examples


```yql
SELECT RemoveTimezone(TzDatetime("2018-02-01T12:00:00,Europe/Moscow"));
```


## Version {#version}

`Version()` returns a string describing the current version of the node processing the query. In some cases, for example, during a gradual cluster update, it may return different strings depending on which node processes the query. The function takes no arguments.

### Examples


```yql
SELECT Version();
```


## MAX_OF, MIN_OF, GREATEST, and LEAST {#max-min}

Returns the minimum or maximum among N arguments. These functions allow you to avoid using the standard SQL construct `CASE WHEN a < b THEN a ELSE b END`, which would be especially cumbersome for N greater than two.

### Signatures


```yql
MIN_OF(T[,T,...})->T
MAX_OF(T[,T,...})->T
```


Argument types must be castable to each other and may allow the value `NULL`.

`GREATEST` is a synonym for `MAX_OF`, and `LEAST` is a synonym for `MIN_OF`.

### Examples


```yql
SELECT MIN_OF(1, 2, 3);
```


## AsTuple, AsStruct, AsList, AsDict, AsSet, AsListStrict, AsDictStrict, and AsSetStrict {#as-container}

Creates containers of the corresponding types. The [operator notation](#containerliteral) for container literals is also available.

Features:

* Container elements are passed as arguments, so the number of elements in the resulting container equals the number of arguments passed, except when dictionary keys are repeated.
* `AsTuple` and `AsStruct` can be called without arguments, and arguments can have different types.
* Field names in `AsStruct` are specified using `AsStruct(field_value AS field_name)`.
* To create a list, at least one argument is required if you need to infer element types. To create an empty list with a given element type, use the [ListCreate](list.md#listcreate) function. You can create an empty list by calling `AsList()` without arguments; in this case, the expression will have type `EmptyList`.
* To create a dictionary, at least one argument is required if you need to infer element types. To create an empty dictionary with a given element type, use the [DictCreate](dict.md#dictcreate) function. You can create an empty dictionary by calling `AsDict()` without arguments; in this case, the expression will have type `EmptyDict`.
* To create a set, at least one argument is required if you need to infer element types. To create an empty set with a given element type, use the [SetCreate](dict.md#setcreate) function. You can create an empty set by calling `AsSet()` without arguments; in this case, the expression will have type `EmptyDict`.
* `AsList` infers the common type of list elements. If types are incompatible, a typing error is generated.
* `AsDict` separately infers the common types of keys and values. If types are incompatible, a typing error is generated.
* `AsSet` infers the common types of keys. If types are incompatible, a typing error is generated.
* `AsListStrict`, `AsDictStrict`, and `AsSetStrict` require the same type for arguments.
* `AsDict` and `AsDictStrict` expect `Tuple` of two elements as arguments: a key and a value, respectively. If keys are repeated, only the value for the first key remains in the dictionary.
* `AsSet` and `AsSetStrict` expect keys as arguments.

### Examples


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

For some containers, an operator form of writing their literal values is possible:

* Tuple — `(value1, value2...)`
* Structure — `<|name1: value1, name2: value2...|>`
* List — `[value1, value2,...]`
* Dictionary — `{key1: value1, key2: value2...}`
* Set — `{key1, key2...}`.

In all cases, a trailing comma is allowed. For a tuple with one element, this comma is mandatory — `(value1,)`.
For field names in a structure literal, you can use an expression that can be evaluated at evaluation time, for example, string literals, as well as identifiers (including in backticks).

Internally, a list uses the [AsList](#as-container) function, a dictionary uses [AsDict](#as-container), a set uses [AsSet](#as-container), a tuple uses [AsTuple](#as-container), and a structure uses [AsStruct](#as-container).

### Examples


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

### Signature


```yql
Variant(T, String, Type<Variant<...>>)->Variant<...>
```


Arguments:

* Value
* String with the field name or tuple index
* Variant type

### Example


```yql
$var_type = Variant<foo: Int32, bar: Bool>;

SELECT
   Variant(6, "foo", $var_type) as Variant1Value,
   Variant(false, "bar", $var_type) as Variant2Value;
```


## AsVariant {#asvariant}

`AsVariant()` creates a value of a [variant over a structure](../types/containers.md) with one field. This value can be implicitly converted to any variant over a structure where the data type matches for this field name and there may be additional fields with other names.

### Signature


```yql
AsVariant(T, String)->Variant
```


Arguments:

* Value
* String with the field name

### Example


```yql
SELECT
   AsVariant(6, "foo") as VariantValue
```


## Visit, VisitOrDefault {#visit}

Processes the possible values of a variant represented by a structure or tuple, using the provided handler functions for each of its fields/elements.

### Signature


```yql
Visit(Variant<key1: K1, key2: K2, ...>, K1->R AS key1, K2->R AS key2, ...)->R
Visit(Variant<K1, K2, ...>, K1->R, K2->R, ...)->R

VisitOrDefault(Variant<K1, K2, ...>{Flags:AutoMap}, R, [K1->R, [K2->R, ...]])->R
VisitOrDefault(Variant<key1: K1, key2: K2, ...>{Flags:AutoMap}, R, [K1->R AS key1, [K2->R AS key2, ...]])->R
```


### Arguments

* For a variant over a structure, the function takes the variant itself as a positional argument and one named handler argument for each field of that structure.
* For a variant over a tuple, the function takes the variant itself and one handler for each tuple element as positional arguments.
* The `VisitOrDefault` modification takes an additional positional argument (in second place) representing a default value and allows omitting some handlers.

### Example


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

Returns the value of a homogeneous variant (i.e., containing fields/elements of the same type).

### Signature


```yql
VariantItem(Variant<key1: K, key2: K, ...>{Flags:AutoMap})->K
VariantItem(Variant<K, K, ...>{Flags:AutoMap})->K
```


### Example


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

Returns the active field (active index) of a variant over a structure (tuple).

### Signature


```yql
Way(Variant<key1: K1, key2: K2, ...>{Flags:AutoMap})->Utf8
Way(Variant<K1, K2, ...>{Flags:AutoMap})->Uint32
```


### Example


```yql
$vr = Variant(1, "0", Variant<Int32, String>);
$vrs = Variant(1, "a", Variant<a:Int32, b:String>);


SELECT Way($vr);  -- 0
SELECT Way($vrs); -- "a"

```


## DynamicVariant {#dynamic_variant}

Creates an instance of a homogeneous variant (i.e., containing fields/elements of the same type), where the index or field of the variant can be set dynamically. If the index or field name does not exist, `NULL` is returned.
The inverse function is [VariantItem](#variantitem).

### Signature


```yql
DynamicVariant(item:T,index:Uint32?,Variant<T, T, ...>)->Optional<Variant<T, T, ...>>
DynamicVariant(item:T,index:Utf8?,Variant<key1: T, key2: T, ...>)->Optional<Variant<key1: T, key2: T, ...>>
```


### Example


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

### Signature


```yql
Enum(String, Type<Enum<...>>)->Enum<...>
```


Arguments:

* String with field name
* Enumeration type

### Example


```yql
$enum_type = Enum<Foo, Bar>;
SELECT
   Enum("Foo", $enum_type) as Enum1Value,
   Enum("Bar", $enum_type) as Enum2Value;
```


## AsEnum {#asenum}

`AsEnum()` creates a value of [enumeration](../types/containers.md) with one element. This value can be implicitly converted to any enumeration containing such a name.

### Signature


```yql
AsEnum(String)->Enum<'tag'>
```


Arguments:

* String with the name of the enumeration element

### Example


```yql
SELECT
   AsEnum("Foo");
```


## AsTagged, Untag {#as-tagged}

Wraps a value into a [Tagged data type](../types/special.md) with the specified label while preserving the physical data type. `Untag` is the reverse operation.

### Signature


```yql
AsTagged(T, tagName:String)->Tagged<T,tagName>
AsTagged(T?, tagName:String)->Tagged<T,tagName>?

Untag(Tagged<T, tagName>)->T
Untag(Tagged<T, tagName>?)->T?
```


Required arguments:

1. Value of an arbitrary type
2. Label name.

Returns a copy of the value from the first argument with the specified label in the data type.

Examples of use cases:

* Returning media files from base64-encoded strings to the client for display in the web interface{% if feature_webui %}. Support for labels in the YQL web UI is [described here](../interfaces/web_tagged.md){% endif %}.

{% if feature_mapreduce %}

* Protection at UDF call boundaries against passing incorrect values.

{% endif %}

* Additional clarifications at the level of the returned column types.

{% if feature_bulk_tables %}

## TablePath {#tablepath}

Access to the current table name, which is often needed when using [CONCAT](../syntax/select/concat.md#concat), [RANGE](../syntax/select/concat.md#range), and other similar mechanisms.

### Signature


```yql
TablePath()->String
```


No arguments. Returns a string with the full path, or an empty string and a warning when used in an unsupported context (for example, when working with a subquery or a range of 1000+ tables).

{% note info %}

The [TablePath](#tablepath), [TableName](#tablename), and [TableRecordIndex](#tablerecordindex) functions do not work for temporary and anonymous tables (return an empty string or 0 for [TableRecordIndex](#tablerecordindex)).
These functions are computed at the moment of [execution](../syntax/select/index.md#selectexec) of the projection in `SELECT`, and by that time the current table may already be temporary.
To avoid this situation, place the computation of these functions into a subquery, as done in the second example below.

{% endnote %}

### Examples


```yql
SELECT TablePath() FROM CONCAT(table_a, table_b);
```


```yql
SELECT key, tpath_ AS path FROM (SELECT a.*, TablePath() AS tpath_ FROM RANGE(`my_folder`) AS a)
WHERE key IN $subquery;
```


## TableName {#tablename}

Get the table name from the table path. The path can be obtained through the [TablePath](#tablepath) function, or as a column `Path` when using the table function {% if feature_mapreduce %}[FOLDER](../syntax/select/folder.md){% else %} `FOLDER`{% endif %}.

### Signature


```yql
TableName()->String
TableName(String)->String
TableName(String, String)->String
```


Optional arguments:

* path to the table, by default `TablePath()` is used (also see its limitations).
* specifying the system ("yt") according to which the table name is derived. Specifying the system is only necessary if the current cluster is not specified using {% if feature_mapreduce %}[USE](../syntax/use.md){% else %}`USE`{% endif %}.

### Examples


```yql
USE hahn;
SELECT TableName() FROM CONCAT(table_a, table_b);
```


```yql
SELECT TableName(Path, "yt") FROM cluster.FOLDER(folder_name);
```


## TableRecordIndex {#tablerecordindex}

Access to the current row number in the source physical table, **starting from 1** (depends on the storage implementation).

### Signature


```yql
TableRecordIndex()->Uint64
```


No arguments. When used in combination with [CONCAT](../syntax/select/concat.md#concat), [RANGE](../syntax/select/concat.md#range) and other similar mechanisms, numbering restarts for each table on input. If used in an incorrect context, returns 0.

### Example


```yql
SELECT TableRecordIndex() FROM my_table;
```

{% endif %}

## TableRow{% if feature_join %}, JoinTableRow{% endif %} {#tablerow}

Retrieving the entire table row as a structure. No arguments{% if feature_join %}. `JoinTableRow` in the case of `JOIN`s always returns a structure with table prefixes{% endif %}.

### Signature


```yql
TableRow()->Struct
```


### Example


```yql
SELECT TableRow() FROM my_table;
```


{% if feature_mapreduce %}

## FileContent and FilePath {#file-content-path}

{% if oss != true %}

Both the [console](../interfaces/cli.md) and [web](../interfaces/web.md)-interfaces allow you to "attach" arbitrary named files to a query. Using these functions, you can get the contents or path of an attached file by its name in the "sandbox" and then use it in the query in any way.

{% endif %}

### Signatures


```yql
FilePath(String)->String
FileContent(String)->String
```


Arguments `FileContent` and `FilePath` are strings with aliases.

### Examples


```yql
SELECT "Content of "
  || FilePath("my_file.txt")
  || ":\n"
  || FileContent("my_file.txt");
```


## FolderPath {#folderpath}

Getting the path to the root of a directory with several "attached" files with the specified common prefix.

### Signature


```yql
FolderPath(String)->String
```


Argument: a string with a prefix among aliases.

Also see [PRAGMA File](../syntax/pragma.md#file) and [PRAGMA Folder](../syntax/pragma.md#folder).

### Examples


```yql
PRAGMA File("foo/1.txt", "http://url/to/somewhere");
PRAGMA File("foo/2.txt", "http://url/to/somewhere/else");
PRAGMA File("bar/3.txt", "http://url/to/some/other/place");

SELECT FolderPath("foo"); -- в директории по возвращённому пути будут
                          -- находиться файлы 1.txt и 2.txt, скачанные по указанным выше ссылкам
```


## ParseFile

Get a list of values from an attached text file. Can be used in combination with [IN](../syntax/expressions.md#in) and attaching a file by URL{% if oss != true %} (instructions for attaching files for {% if feature_webui %}[web interface](../interfaces/web.md#attach) and {% endif %} [client](../interfaces/cli.md#attach)){% endif %}.

Only one file format is supported — one value per line.{% if feature_udf_noncpp and oss != true %} For something more complex, you will have to write a small UDF in [Python](../udf/python.md) or [JavaScript](../udf/javascript.md).{% endif %}

### Signature


```yql
ParseFile(String, String)->List<T>
```


Two required arguments:

1. List cell type: only strings and numeric types are supported.
2. Name of the attached file.

{% note info %}

The return value is a lazy list. To reuse it, wrap it in the [ListCollect](list.md#listcollect) function.

{% endnote %}

### Examples


```yql
SELECT ListLength(ParseFile("String", "my_file.txt"));
```


```yql
SELECT * FROM my_table
WHERE int_column IN ParseFile("Int64", "my_file.txt");
```


## WeakField {#weakfield}

Extracts a table column from a strict schema, if it exists there, or from the `_other` and `_rest` fields. If the value is missing, `NULL` is returned.

Syntax: `WeakField([<table>.]<field>, <type>[, <default_value>])`.

The default value is used only if the column is missing from the data schema. To substitute a default value in any case, use [COALESCE](#coalesce).

### Examples


```yql
SELECT
    WeakField(my_column, String, "no value"),
    WeakField(my_table.other_column, Int64)
FROM my_table;
```

{% endif %}

## Ensure... {#ensure}

Checking user conditions:

* `Ensure()` — checking that the predicate is true during query execution.
* `EnsureType()` — checking that the expression type exactly matches the specified type.
* `EnsureConvertibleTo()` — soft checking of expression type compatibility, following the same rules as implicit type casting.

If the check fails, the entire query terminates with an error.

### Signatures


```yql
Ensure(T, Bool, String)->T
EnsureType(T, Type<T>, String)->T
EnsureConvertibleTo(T, Type<T>, String)->T
```


Arguments:

1. The expression that will become the result of the function call if the check succeeds. It is also subject to data type checking in the corresponding functions.
2. In Ensure — a Boolean predicate that is checked against `true`. In the other functions — a data type that can be obtained via [the functions designed for this purpose](types.md), or a string literal with [a textual type description](../types/type_string.md).
3. An optional string with an error comment that will be included in the overall error message when the query terminates. For type checks, it cannot use the data itself, as they are performed at the query validation stage, while for Ensure it can be an arbitrary expression.

{% if backend_name != "YDB" %}

To check conditions on the final computation result of Ensure, it is convenient to use it in combination with [DISCARD SELECT](../syntax/discard.md).

{% endif %}

### Examples


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


## AssumeStrict {#assumestrict}

### Signature


```yql
AssumeStrict(T)->T
```


The `AssumeStrict` function returns its argument. Using this function is a way to tell the YQL optimizer that the expression in the argument is *strict*, i.e., free of runtime errors.
Most built-in YQL functions and operators are strict, but there are exceptions – for example [Unwrap](#optional-ops) and [Ensure](#ensure).
In addition, a UDF call is considered a non-strict expression.

If you are confident that no runtime errors actually occur when evaluating the expression, it makes sense to use `AssumeStrict`.

### Example


```yql
SELECT * FROM T1 AS a JOIN T2 AS b USING(key)
WHERE AssumeStrict(Unwrap(CAST(a.key AS Int32))) == 1;
```


In this example, we assume that all values of the text column `a.key` in the `T1` table are valid numbers, so Unwrap does not cause an error.
With `AssumeStrict`, the optimizer can perform filtering first and then JOIN.
Without `AssumeStrict`, such optimization is not performed – the optimizer must account for the situation where the `a.key` column contains non-numeric values that are filtered out by the `JOIN` filter.

## Likely {#likely}

### Signature


```yql
Likely(Bool)->Bool
Likely(Bool?)->Bool?
```


The `Likely` function returns its argument. The function is a hint to the optimizer and indicates that in most cases its argument will have the value `True`.
For example, the presence of such a function in `WHERE` means that the filter is low-selectivity.

### Example


```yql
SELECT * FROM T1 AS a JOIN T2 AS b USING(key)
WHERE Likely(a.amount > 0)  -- almost always true
```


With `Likely`, the optimizer will not try to perform filtering before `JOIN`.

{% if feature_codegen %}

## EvaluateExpr, EvaluateAtom {#evaluate_expr_atom}

The ability to evaluate an expression before the main computation starts and substitute its result into the query as a literal (constant). In many contexts where standard SQL would expect only a constant (for example, in table names, the number of rows in [LIMIT](../syntax/select/limit_offset.md), etc.), this functionality is activated implicitly and automatically.

EvaluateExpr can be used in places where the grammar already expects an expression. For example, it can be used to:

* round the current time to days, weeks, or months and substitute it into the query, which will then allow [query caching](../syntax/pragma.md#yt.querycachemode) to work correctly, although using [functions for obtaining the current time](#current-utc) usually disables it completely.
* perform a heavy computation with a small result once per query instead of once per job.

{% if backend_name == "YT" %}

EvaluateAtom allows you to dynamically create an [atom](../types/special.md), but since it is mainly operated by the lower level of [s-expressions](/docs/s_expressions/functions), it is generally not recommended to use this function directly.

{% endif %}

The only argument for both functions is the expression itself for evaluation and substitution.

Limitations:

* the expression must not trigger MapReduce operations.
* this functionality is completely blocked in YQL over YDB.

### Examples


```yql
$now = CurrentUtcDate();
SELECT EvaluateExpr(
    DateTime::MakeDate(DateTime::StartOfWeek($now)
    )
);
```

{% endif %}

## Literals of simple types {#data-type-literals}

For simple types, literals can be created based on string literals.

### Syntax

`<Simple type>( <string>[, <additional attributes>] )`

Unlike `CAST("myString" AS MyType)`:

* Checking whether the literal can be cast to the required type occurs at the validation stage.
* The result is not optional.

For data types `Date`, `Datetime`, `Timestamp`, and `Interval`, only literals in the format conforming to [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) are supported. `Interval` has the following differences from the standard:

* a negative sign for shifts into the past is supported.
* microseconds can be written as a fractional part of seconds.
* units of measurement larger than weeks are not available.
* variants with interval start/end and repetitions are not supported.

For data types `TzDate`, `TzDatetime`, `TzTimestamp`, literals are also specified in the format conforming to [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), but instead of the optional Z suffix, an [IANA time zone name](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) is specified after a comma, for example, GMT or Europe/Moscow.

{% include [decimal args](../_includes/decimal_args.md) %}

{% include [x](../_includes/type_literals_examples.md) %}

{% if feature_webui %}

## Accessing metadata of the current operation {#metadata}

When running YQL operations through the web interface or HTTP API, access to the following information is provided:

* `CurrentOperationId()` — private operation identifier.
* `CurrentOperationSharedId()` — public operation identifier.
* `CurrentAuthenticatedUser()` — current user login.

### Signatures


```yql
CurrentOperationId()->String
CurrentOperationSharedId()->String
CurrentAuthenticatedUser()->String
```


No arguments.

If this information is absent, for example, when running in embedded mode, an empty string is returned.

### Examples


```yql
SELECT
    CurrentOperationId(),
    CurrentOperationSharedId(),
    CurrentAuthenticatedUser();
```

{% endif %}

## ToBytes and FromBytes {#to-from-bytes}

Conversion of [simple data types](../types/primitive.md) to a string with their binary representation and back. Numbers are represented in [little endian](https://en.wikipedia.org/wiki/Endianness#Little-endian).

### Signatures


```yql
ToBytes(T)->String
ToBytes(T?)->String?

FromBytes(String, Type<T>)->T?
FromBytes(String?, Type<T>)->T?
```


### Examples


```yql
SELECT
    ToBytes(123), -- "{\u0000\u0000\u0000"
    String::HexEncode(ToBytes(123)) -- "7B000000"
    FromBytes(
        "\xd2\x02\x96\x49\x00\x00\x00\x00",
        Uint64
    ); -- 1234567890ul
```


## ByteAt {#byteat}

Getting the value of a byte in a string by index from its beginning. If the index is invalid, `NULL` is returned.

### Signature


```yql
ByteAt(String, Uint32)->Uint8
ByteAt(String?, Uint32)->Uint8?

ByteAt(Utf8, Uint32)->Uint8
ByteAt(Utf8?, Uint32)->Uint8?
```


Arguments:

1. String: `String` or `Utf8`.
2. Index: `Uint32`.

### Examples


```yql
SELECT
    ByteAt("foo", 0), -- 102
    ByteAt("foo", 1), -- 111
    ByteAt("foo", 9); -- NULL
```


## ...Bit {#bitops}

`TestBit()`, `ClearBit()`, `SetBit()`, and `FlipBit()` - check, clear, set, or invert a bit in an unsigned number by the specified bit index.

### Signatures


```yql
TestBit(T, Uint8)->Bool
TestBit(T?, Uint8)->Bool?

ClearBit(T, Uint8)->T
ClearBit(T?, Uint8)->T?

SetBit(T, Uint8)->T
SetBit(T?, Uint8)->T?

FlipBit(T, Uint8)->T
FlipBit(T?, Uint8)->T?
```


Arguments:

1. An unsigned number on which to perform the required operation. TestBit is also implemented for strings.
2. Bit number.

TestBit returns `true/false`. The other functions return a copy of their first argument with the corresponding transformation applied.

### Examples


```yql
SELECT
    TestBit(1u, 0), -- true
    SetBit(8u, 0); -- 9
```


## Abs {#abs}

Absolute value of a number.

### Signature


```yql
Abs(T)->T
Abs(T?)->T?
```


### Examples


```yql
SELECT Abs(-123); -- 123
```


## Just {#optional-ops}

`Just()` — Change the data type of a value to [optional](../types/optional.md) of the current data type (i.e., `T` becomes `T?`).

### Signature


```yql
Just(T)->T?
```


### Examples


```yql
SELECT
  Just("my_string"); --  String?
```


## Unwrap {#unwrap}

`Unwrap()` — Convert an [optional](../types/optional.md) data type value to the corresponding non-optional type with a runtime error if the data contains `NULL`. Thus, `T?` becomes `T`.

If the value is not [optional](../types/optional.md), the function returns its first argument unchanged.

### Signature


```yql
Unwrap(T?)->T
Unwrap(T?, Utf8)->T
Unwrap(T?, String)->T
```


Arguments:

1. Value to convert.
2. Optional string with a comment for the error text.

The reverse operation is [Just](#optional-ops).

### Examples


```yql
$value = Just("value");

SELECT Unwrap($value, "Unexpected NULL for $value");
```


## Nothing {#nothing}

`Nothing()` — Create an empty value of the specified [Optional](../types/optional.md) data type.

### Signature


```yql
Nothing(Type<T?>)->T?
```


### Examples


```yql
SELECT
  Nothing(String?); -- empty value (NULL) with type String?
```


[More about ParseType and other functions for working with data types](types.md).

## Callable {#callable}

Create a callable value with a given signature from a lambda function. Usually used to place callable values in containers.

### Signature


```yql
Callable(Type<Callable<(...)->T>>, lambda)->Callable<(...)->T>
```


Arguments:

1. Type.
2. Lambda function.

### Examples


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

`Pickle()` and `StablePickle()` serialize an arbitrary object into a byte sequence, if possible. Typical non-serializable objects are Callable and Resource. The serialization format is not versioned; it is allowed to use within a single query. For the Dict type, the StablePickle function pre-sorts the keys, while for Pickle, the order of dictionary elements in the serialized representation is undefined.

`Unpickle()` — the reverse operation (deserialization), where the first argument is the result data type, and the second is a string with the result of `Pickle()` or `StablePickle()`.

### Signatures


```yql
Pickle(T)->String
StablePickle(T)->String
Unpickle(Type<T>, String)->T
```


### Examples

{% if feature_tablesample==true %}

```yql
SELECT *
FROM my_table
WHERE Digest::MurMurHash32(
        Pickle(TableRow())
    ) % 10 == 0; -- в реальности лучше использовать TABLESAMPLE

$buf = Pickle(123);
SELECT Unpickle(Int32, $buf);
```

{% else %}

```yql
SELECT *
FROM my_table
WHERE Digest::MurMurHash32(
        Pickle(TableRow())
    ) % 10 == 0;

$buf = Pickle(123);
SELECT Unpickle(Int32, $buf);
```

{% endif %}

## StaticMap

Transforms a structure or tuple by applying a lambda to each element.

### Signature


```yql
StaticMap(Struct<...>, lambda)->Struct<...>
StaticMap(Tuple<...>, lambda)->Tuple<...>
```


Arguments:

* Structure or tuple;
* Lambda for processing elements.

Result: a structure or tuple with the same number and naming of elements as the first argument, and the data types of the elements are determined by the lambda results.

### Examples


```yql
SELECT *
FROM (
    SELECT
        StaticMap(TableRow(), ($item) -> {
            return CAST($item AS String);
        })
    FROM my_table
) FLATTEN COLUMNS; -- converting all columns to strings
```


## StaticZip

Element-wise "glues" structures or tuples. All arguments (one or more) must be either structures with the same set of fields, or tuples of the same length.
The result will be a structure or tuple accordingly.
Each element of the result is a tuple with the corresponding elements from the arguments.

### Signature


```yql
StaticZip(Struct, Struct)->Struct
StaticZip(Tuple, Tuple)->Tuple
```


### Examples


```yql
$one = <|k1:1, k2:2.0|>;
$two = <|k1:3.0, k2:4|>;

-- element-wise addition of two structures
SELECT StaticMap(StaticZip($one, $two), ($tuple)->($tuple.0 + $tuple.1)) AS sum;
```


## StaticFold, StaticFold1 {#staticfold}


```yql
StaticFold(obj:Struct/Tuple, initVal, updateLambda)
StaticFold1(obj:Struct/Tuple, initLambda, updateLambda)
```


Static left-associative fold of a structure or tuple.
For tuples, the fold is performed in order from the smallest index to the largest; for structures, the order is not guaranteed.

- `obj` - object whose elements need to be folded
- `initVal` - *(for StaticFold)* initial state of the fold
- `initLambda` - *(for StaticFold1)* function to obtain the initial state from the first element
- `updateLambda` - state update function (takes the next element of the object and the previous state as arguments)

`StaticFold(<|key_1:$el_1, key_2:$el_2, ..., key_n:$el_n|>, $init, $f)` is transformed into a fold:


```yql
$f($el_n, ...$f($el_2, $f($init, el_1))...)
```


`StaticFold1(<|key_1:$el_1, key_2:$el_2, ..., key_n:$el_n|>, $f0, $f)`:


```yql
$f($el_n, ...$f($el_2, $f($f0($init), el_1))...)
```


`StaticFold1(<||>, $f0, $f)` will return `NULL`.

It works similarly with tuples.

## AggregationFactory {#aggregationfactory}

Create a factory for [aggregate functions](aggregation.md) to separate the process of describing how to aggregate data from the data to which it is applied.

Arguments:

1. A quoted string that is the name of an aggregate function, for example ["MIN"](aggregation.md#min).
2. Optional parameters of the aggregate function that do not depend on the data. For example, the percentile value in [PERCENTILE](aggregation.md#percentile).

The resulting factory can be used as the second parameter of the [AGGREGATE_BY](aggregation.md#aggregateby) function.
If the aggregate function works on two columns instead of one, such as [MIN_BY](aggregation.md#minby), then the first argument of [AGGREGATE_BY](aggregation.md#aggregateby) is a `Tuple` of two values. This is detailed in the description of such an aggregate function.

### Examples


```yql
$factory = AggregationFactory("MIN");
SELECT
    AGGREGATE_BY(value, $factory) AS min_value -- apply MIN aggregation to the value column
FROM my_table;
```


## AggregateTransformInput {#aggregatetransform}

`AggregateTransformInput()` transforms a factory for [aggregate functions](aggregation.md), for example obtained via the [AggregationFactory](#aggregationfactory) function, into another factory in which the specified transformation of input elements is performed before starting the aggregation.

Arguments:

1. Factory for aggregate functions;
2. Lambda function with one argument that transforms the input element.

### Examples


```yql
$f = AggregationFactory("sum");
$g = AggregateTransformInput($f, ($x) -> (cast($x as Int32)));
$h = AggregateTransformInput($f, ($x) -> ($x * 2));
SELECT ListAggregate([1,2,3], $f); -- 6
SELECT ListAggregate(["1","2","3"], $g); -- 6
SELECT ListAggregate([1,2,3], $h); -- 12
```


## AggregateTransformOutput {#aggregatetransformoutput}

`AggregateTransformOutput()` transforms a factory for [aggregate functions](aggregation.md), for example obtained via the [AggregationFactory](#aggregationfactory) function, into another factory in which the specified transformation of the result is performed after the aggregation completes.

Arguments:

1. Factory for aggregate functions;
2. Lambda function with one argument that transforms the result.

### Examples


```yql
$f = AggregationFactory("sum");
$g = AggregateTransformOutput($f, ($x) -> ($x * 2));
SELECT ListAggregate([1,2,3], $f); -- 6
SELECT ListAggregate([1,2,3], $g); -- 12
```


## AggregateFlatten {#aggregateflatten}

Adapts a factory for [aggregate functions](aggregation.md) (for example, obtained via the [AggregationFactory](#aggregationfactory) function) to perform aggregation over input elements that are lists. This operation is similar to [FLATTEN LIST BY](../syntax/select/flatten.md) — aggregation is performed on each element of the list.

Arguments:

1. Factory for aggregate functions.

### Examples


```yql
$i = AggregationFactory("AGGREGATE_LIST_DISTINCT");
$j = AggregateFlatten($i);
SELECT AggregateBy(x, $j) from (
   SELECT [1,2] as x
   union all
   SELECT [2,3] as x
); -- [1, 2, 3]

```


{% if tech %}

## YQL::, s-expressions {#s-expressions}

The full list of internal YQL functions is in the [s-expressions documentation](/docs/s_expressions/functions), an alternative low-level YQL syntax. Any of the functions listed there can also be called from SQL syntax by adding the `YQL::` prefix to its name, but this is not recommended because this mechanism is primarily intended for temporary workarounds of possible issues and for internal testing needs.

If a function is available in SQL syntax without the `YQL::` prefix, its behavior may differ from the function with the same name in the s-expressions documentation, if such a function exists.

{% endif %}
