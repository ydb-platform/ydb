# SimplePg

The SimplePg module provides access to some functions from the PostgreSQL codebase, with input and output arguments adapted to [primitive types](../../types/primitive.md).
By default, these functions require the module name to be specified as `SimplePg::foo`, but by adding PRAGMA `SimplePg`, they can be used in the global scope as `foo` (which guarantees that they will override the built-in YQL function, if any).

Many of the functions in this module have equivalents among other YQL functions with better performance.

## now

#### Signature

```yql
SimplePg::now() -> Timestamp?
```

Available since version [2025.04](../../changelog/2025.04.md#simple-pg-module).
No arguments.
Returns the current time.
Original [documentation](https://www.postgresql.org/docs/16/functions-datetime.html).
Equivalent to [CurrentUtcTimestamp](../../builtins/basic.md#current-utc).

#### Examples

```yql
PRAGMA SimplePg;
SELECT now();
```

## to_date

#### Signature

```yql
SimplePg::to_date(Utf8?,Utf8?) -> Date32?
```

Available since version [2025.04](../../changelog/2025.04.md#simple-pg-module).

Arguments:
* Date value string;
* Format string.

Parses the date according to the format string.
Original [documentation](https://www.postgresql.org/docs/16/functions-formatting.html).
Equivalent to [DateTime::Parse64](datetime.md#parse).

#### Examples

```yql
PRAGMA SimplePg;
SELECT to_date('05 Dec 2000', 'DD Mon YYYY'); -- 2000-12-05
```

## to_char

#### Signature

```yql
SimplePg::to_char(AnyPrimitiveType?,Utf8?) -> Utf8?
```

Available since version [2025.04](../../changelog/2025.04.md#simple-pg-module).

Arguments:
* Primitive type value;
* Format string.

Formats the value according to the format string.
Original [documentation](https://www.postgresql.org/docs/16/functions-formatting.html).
Equivalent to [DateTime::Format](datetime.md#format).

#### Examples

```yql
PRAGMA SimplePg;
SELECT to_char(125.8, '999D9'); -- "125.8"
SELECT to_char(Timestamp('2002-04-20T17:31:12.66Z'), 'HH12:MI:SS'); -- 05:31:12
```

## date_part

#### Signature

```yql
SimplePg::date_part(Utf8?,Timestamp64?) -> Double?
SimplePg::date_part(Utf8?,Interval64?) -> Double?
```

Available since version [2025.04](../../changelog/2025.04.md#simple-pg-module).

Arguments:
* Component, possible [values](https://www.postgresql.org/docs/16/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT);
* Timestamp.

Extracts the specified component from a timestamp or interval.
Original [documentation](https://www.postgresql.org/docs/16/functions-datetime.html).
Equivalent to [DateTime::Get](datetime.md#get).

#### Examples

```yql
PRAGMA SimplePg;
SELECT date_part('hour', Timestamp('2001-02-16T20:38:40Z')); -- 20
SELECT date_part('minute', Interval('PT01H02M03S')); -- 2
```

## date_trunc

#### Signature

```yql
SimplePg::date_trunc(Utf8?,Timestamp64?) -> Timestamp64?
```

Available since version [2025.04](../../changelog/2025.04.md#simple-pg-module).

Arguments:
* Scale, possible [values](https://www.postgresql.org/docs/16/functions-datetime.html#FUNCTIONS-DATETIME-TRUNC);
* Timestamp.

Rounds the timestamp to the start of the specified scale.
Original [documentation](https://www.postgresql.org/docs/16/functions-datetime.html).
Equivalent to [DateTime::StartOf](datetime.md#startof), etc.

#### Examples

```yql
PRAGMA SimplePg;
SELECT date_trunc('hour', Timestamp('2001-02-16T20:38:40Z')); -- 2001-02-16 20:00:00
SELECT date_trunc('year', Timestamp('2001-02-16T20:38:40Z')); -- 2001-01-01 00:00:00
```

## floor

#### Signature

```yql
SimplePg::floor(Double?) -> Double?
```

Available since version [2025.04](../../changelog/2025.04.md#simple-pg-module).

Arguments:
* Floating-point value;

Rounds a value down to the nearest integer.
Original [documentation](https://www.postgresql.org/docs/16/functions-math.html).
Analog - [Math::Floor](math.md).

#### Examples

```yql
PRAGMA SimplePg;
SELECT floor(42.8); -- 42
SELECT floor(-42.8); -- -43
```

## ceil

#### Signature

```yql
SimplePg::ceil(Double?) -> Double?
```

Available since version [2025.04](../../changelog/2025.04.md#simple-pg-module).

Arguments:
* Floating-point value;

Rounds a value up to the nearest integer.
Original [documentation](https://www.postgresql.org/docs/16/functions-math.html).
Equivalent to [Math::Ceil](math.md).

#### Examples

```yql
PRAGMA SimplePg;
SELECT ceil(42.2); -- 43
SELECT ceil(-42.8); -- -42
```

## round

#### Signature

```yql
SimplePg::round(Double?,[Int32?]) -> Double?
```

Available since version [2025.04](../../changelog/2025.04.md#simple-pg-module).

Arguments:
* Floating-point value;
* Optional number of decimal places (default - 0).

Rounds a value to the specified number of decimal places. Rounding away from zero is used.
Original [documentation](https://www.postgresql.org/docs/16/functions-math.html).
Equivalent to [Math::Round](math.md).

#### Examples

```yql
PRAGMA SimplePg;
SELECT round(42.4382, 2); -- 42.44
SELECT round(1234.56, -1); -- 1230
```
