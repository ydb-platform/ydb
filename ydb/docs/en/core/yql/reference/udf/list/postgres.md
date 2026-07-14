# PostgreSQL UDF

<!-- markdownlint-disable blanks-around-fences -->

[YQL](../../index.md) provides access to [functions](https://www.postgresql.org/docs/16/functions.html) and [data types](https://www.postgresql.org/docs/16/datatype.html) of PostgreSQL.

PostgreSQL type names in YQL are obtained by adding the `Pg` prefix to the original type name.
For example `PgVarchar`, `PgInt4`, `PgText`. pg type names (like all type names) in YQL are case-insensitive. Currently, all simple PostgreSQL data types and arrays are supported.

If the original type is an array type (in PostgreSQL such types start with an underscore: `_int4` - array of 32-bit integers), then the type name in YQL also starts with an underscore – `_PgInt4`.

## Literals {#literals}

String and numeric literals of Pg types can be created using special suffixes (similar to simple [string](../../syntax/lexer.md#string-literals) and [numeric](../../syntax/lexer.md#literal-numbers) literals).

### Integer literals {#intliterals}

| Suffix | Type | Comment |
| --- | --- | --- |
| `p` | `PgInt4` | 32-bit signed integer (PostgreSQL has no unsigned types) |
| `ps` | `PgInt2` | 16-bit signed integer |
| `pi` | `PgInt4` |  |
| `pb` | `PgInt8` | 64-bit signed integer |
| `pn` | `PgNumeric` | signed integer of arbitrary precision (up to 131072 digits) |

### Floating-point literals {#floatliterals}

| Suffix | Type | Comment |
| --- | --- | --- |
| `p` | `PgFloat8` | floating-point number (64-bit double) |
| `pf4` | `PgFloat4` | floating-point number (32-bit float) |
| `pf8` | `PgFloat8` |  |
| `pn` | `PgNumeric` | floating-point number of arbitrary precision (up to 131072 digits before the decimal point, up to 16383 digits after the decimal point) |

### String literals {#stringliterals}

| Suffix | Type | Comment |
| --- | --- | --- |
| `p` | `PgText` | text string |
| `pt` | `PgText` |  |
| `pv` | `PgVarchar` | text string |
| `pb` | `PgBytea` | binary string |

{% note warning "Warning" %}

The values of string/numeric literals (i.e., what comes before the suffix) must be a valid string/number from the YQL perspective.
In particular, YQL escaping rules must be followed, not PostgreSQL.

{% endnote %}

Example:


```yql
SELECT
    1234p,       -- pgint4
    0x123pb,     -- pgint8
    "тест"pt,    -- pgtext
    123e-1000pn; -- pgnumeric
;
```


### Array literal

The `PgArray` function is used to construct an array literal:


```yql
SELECT
    PgArray(1p, NULL ,2p) -- {1,NULL,2}, type _int4
;
```


### Literal constructor of arbitrary type {#literals_constructor}

Literals of all types (including Pg types) can be created using a literal constructor with the following signature:
`Type_name(<string constant>)`.

For example:


```yql
DECLARE $foo AS String;
SELECT
    PgInt4("1234"), -- same as 1234p
    PgInt4(1234),   -- you can use literal constants as an argument
    PgInt4($foo),   -- and declare parameters
    PgBool(true),
    PgInt8(1234),
    PgDate("1932-01-07"),
;
```


The built-in function `PgConst` with the following signature is also supported: `PgConst(<string value>, <type>)`.
This method is more convenient for code generation.

For example:


```yql
SELECT
    PgConst("1234", PgInt4), -- same as 1234p
    PgConst("true", PgBool)
;
```


For some types, additional modifiers can be specified in the `PgConst` function. Possible modifiers for the `pginterval` type are listed in the [PostgreSQL documentation](https://www.postgresql.org/docs/16/datatype-datetime.html).


```yql
SELECT
    PgConst(90, pginterval, "day"), -- 90 days
    PgConst(13.45, pgnumeric, 10, 1); -- 13.5
;
```


## Operators {#operators}

PostgreSQL operators (unary and binary) are available via the built-in function `PgOp(<operator>, <operands>)`:


```yql
SELECT
    PgOp("*", 123456789987654321pn, 99999999999999999999pn), --  12345678998765432099876543210012345679
    PgOp('|/', 10000.0p), -- 100.0p (square root)
    PgOp("-", 1p), -- -1p
    -1p,           -- unary minus for literals works even without PgOp
;
```


## Type cast operator {#cast_operator}

To cast a value of one Pg type to another, the built-in function `PgCast(<source value>, <desired type>)` is used:


```yql
SELECT
    PgCast(123p, PgText), -- convert number to string
;
```


When converting from string Pg types to some target types, additional modifiers can be specified. Possible modifiers for the `pginterval` type are listed in the [documentation](https://www.postgresql.org/docs/16/datatype-datetime.html).


```yql
SELECT
    PgCast('90'p, pginterval, "day"), -- 90 days
    PgCast('13.45'p, pgnumeric, 10, 1); -- 13.5
;
```


## Converting Pg type values to YQL type values and vice versa {#frompgtopg}

For some Pg types, conversion to YQL types and back is possible. Conversion is performed using the built-in functions
`FromPg(<Pg type value>)` and `ToPg(<YQL type value>)`:


```yql
SELECT
    FromPg("тест"pt), -- Just(Utf8("тест")) - pg types are always nullable
    ToPg(123.45), -- 123.45pf8
;
```


### List of PostgreSQL type aliases when used in YQL {#pgyqltypes}

Below are the YQL data types, their corresponding logical PostgreSQL types, and the PostgreSQL type names when used in YQL:

| YQL | PostgreSQL | PostgreSQL type name in YQL |
| --- | --- | --- |
| `Bool` | `bool` | `pgbool` |
| `Int8` | `int2` | `pgint2` |
| `Uint8` | `int2` | `pgint2` |
| `Int16` | `int2` | `pgint2` |
| `Uint16` | `int4` | `pgint4` |
| `Int32` | `int4` | `pgint4` |
| `Uint32` | `int8` | `pgint8` |
| `Int64` | `int8` | `pgint8` |
| `Uint64` | `numeric` | `pgnumeric` |
| `Float` | `float4` | `pgfloat4` |
| `Double` | `float8` | `pgfloat8` |
| `String` | `bytea` | `pgbytea` |
| `Utf8` | `text` | `pgtext` |
| `Yson` | `bytea` | `pgbytea` |
| `Json` | `json` | `pgjson` |
| `Uuid` | `uuid` | `pguuid` |
| `JsonDocument` | `jsonb` | `pgjsonb` |
| `Date` | `date` | `pgdate` |
| `Datetime` | `timestamp` | `pgtimestamp` |
| `Timestamp` | `timestamp` | `pgtimestamp` |
| `Interval` | `interval` | `pginterval` |
| `TzDate` | `text` | `pgtext` |
| `TzDatetime` | `text` | `pgtext` |
| `TzTimestamp` | `text` | `pgtext` |
| `Date32` | `date` | `pgdate` |
| `Datetime64` | `timestamp` | `pgtimestamp` |
| `Timestamp64` | `timestamp` | `pgtimestamp` |
| `Interval64` | `interval` | `pginterval` |
| `TzDate32` | `text` | `pgtext` |
| `TzDatetime64` | `text` | `pgtext` |
| `TzTimestamp64` | `text` | `pgtext` |
| `Decimal` | `numeric` | `pgnumeric` |
| `DyNumber` | `numeric` | `pgnumeric` |

### Type mapping table `ToPg` {#topg}

Table of correspondence between YQL and PostgreSQL data types when using the `ToPg` function:

{% include [topg](../../_includes/topg.md) %}

### Type mapping table `FromPg` {#frompg}

Table of correspondence between PostgreSQL and YQL data types when using the `FromPg` function:

{% include [frompg](../../_includes/frompg.md) %}

## Calling PostgreSQL functions {#callpgfunction}

To call a PostgreSQL function, you need to add the `Pg::` prefix to its name:


```yql
SELECT
    Pg::extract('isodow'p,PgCast('1961-04-12'p,PgDate)), -- 3pn (Wednesday) - working with dates before 1970
    Pg::generate_series(1p,5p), -- [1p,2p,3p,4p,5p] - for generator functions, a lazy list is returned
;
```


There is also an alternative way to call functions via the built-in function `PgCall(<function name>, <operands>)`:


```yql
SELECT
    PgCall('lower', 'Test'p), -- 'test'p
;
```


When calling a function that returns a set `pgrecord`, you can unpack the result into a list of structures using the `PgRangeCall(<function name>, <operands>)` function:


```yql
SELECT * FROM
    AS_TABLE(PgRangeCall("json_each", pgjson('{"a":"foo", "b":"bar"}')));
    --- 'a'p,pgjson('"foo"')
    --- 'b'p,pgjson('"bar"')
;
```


## Calling aggregate PostgreSQL functions {#pgaggrfunction}

To call an aggregate PostgreSQL function, you need to add the `Pg::` prefix to its name:


```yql
SELECT
Pg::string_agg(x,','p)
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'a,b,c'p

SELECT
Pg::string_agg(x,','p) OVER (ORDER BY x)
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'a'p,'a,b'p,'a,b,c'p
;
```


You can also use an aggregate PostgreSQL function to build an aggregate function factory for subsequent use in `AGGREGATE_BY`:


```yql
$agg_max = AggregationFactory("Pg::max");

SELECT
AGGREGATE_BY(x,$agg_max)
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'c'p

SELECT
AGGREGATE_BY(x,$agg_max) OVER (ORDER BY x),
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'a'p,'b'p,'c'p
```


In this case, the `AggregationFactory` call accepts only the function name with the `Pg::` prefix, and all function arguments are passed to `AGGREGATE_BY`.

If the aggregate function has not one argument but zero or two or more, you must use a tuple when calling `AGGREGATE_BY`:


```yql
$agg_string_agg = AggregationFactory("Pg::string_agg");

SELECT
AGGREGATE_BY((x,','p),$agg_string_agg)
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'a,b,c'p

SELECT
AGGREGATE_BY((x,','p),$agg_string_agg) OVER (ORDER BY x)
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'a'p,'a,b'p,'a,b,c'p
```


{% note warning "Warning" %}

The `DISTINCT` mode over arguments when calling aggregate PostgreSQL functions is not supported, nor is the use of `MULTI_AGGREGATE_BY`.

{% endnote %}

## Logical operations

The functions `PgAnd`, `PgOr`, `PgNot` are used to perform logical operations:


```yql
SELECT
    PgAnd(PgBool(true), PgBool(true)), -- PgBool(true)
    PgOr(PgBool(false), null), -- PgCast(null, pgbool)
    PgNot(PgBool(true)), -- PgBool(false)
;
```
