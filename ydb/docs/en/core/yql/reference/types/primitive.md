# Primitive data types

<!-- markdownlint-disable blanks-around-fences -->

The terms "simple", "primitive", and "elementary" data types are used as synonyms.
## Numeric types {#numeric}

#|
|| Type |
Description |
Notes
    ||
|| `Bool` |
Boolean value. |
    ||
|| `Int8` |
Signed integer.
Valid values: from –2<sup>7</sup> to 2<sup>7</sup>–1. |
    ||
|| `Int16` |
Signed integer.
Valid values: from –2<sup>15</sup> to 2<sup>15</sup>–1. |
    ||
|| `Int32` |
Signed integer.
Valid values: from –2<sup>31</sup> to 2<sup>31</sup>–1. |
    ||
|| `Int64` |
Signed integer.
Valid values: from –2<sup>63</sup> to 2<sup>63</sup>–1. |
    ||
|| `Uint8` |
Unsigned integer.
Valid values: from 0 to 2<sup>8</sup>–1. |
    ||
|| `Uint16` |
Unsigned integer.
Valid values: from 0 to 2<sup>16</sup>–1. |
    ||
|| `Uint32` |
Unsigned integer.
Valid values: from 0 to 2<sup>32</sup>–1. |
    ||
|| `Uint64` |
Unsigned integer.
Valid values: from 0 to 2<sup>64</sup>–1. |
    ||
|| `Float` |
Floating-point number with variable precision, 4 bytes in size. |
{% if feature_map_tables %}Cannot be used in the primary key and in the columns that form the secondary index key{% endif %}
    ||
|| `Double` |
Floating-point number with variable precision, 8 bytes in size. |
{% if feature_map_tables %}Cannot be used in the primary key and in the columns that form the secondary index key{% endif %}
    ||
|| `Decimal(precision, scale)` |
Floating-point number with fixed precision, 16 bytes in size. Precision is the maximum total number of stored decimal digits, takes values from 1 to 35. Scale is the maximum number of stored decimal digits to the right of the decimal point, takes values from 0 to the value of precision. |
    ||
{% if feature_map_tables %}
|| `DyNumber` |
Binary representation of a floating-point number with up to 38 digits of precision.
Valid values: positive from 1×10<sup>-130</sup> to 1×10<sup>126</sup>–1, negative from -1×10<sup>126</sup>–1 to -1×10<sup>-130</sup> and 0.
Compatible with the `Number` type of AWS DynamoDB. Not recommended for use in {{ backend_name_lower }}-native applications. | Not supported in columnar tables
    ||
{% endif %}
|#

{% include [x](../_includes/type_literals_examples.md) %}
## String types {#string}

#|
|| Type | Description | Notes ||
|| `String` |
String, can contain arbitrary binary data |
    ||
|| `Bytes` (alias for `String`) |
String, can contain arbitrary binary data |
    ||
|| `Utf8` |
Text in [UTF-8 encoding](https://en.wikipedia.org/wiki/UTF-8) |
    ||
|| `Text` (alias for `Utf8`) |
Text in [UTF-8 encoding](https://en.wikipedia.org/wiki/UTF-8) |
    ||
|| `Json` |
[JSON](https://en.wikipedia.org/wiki/JSON) in text representation |
Does not support comparison{% if feature_map_tables %}, cannot be used in the primary key and in the columns that form the secondary index key{% endif %}
    ||
|| `JsonDocument` |
[JSON](https://en.wikipedia.org/wiki/JSON) in binary indexed representation |
Does not support comparison{% if feature_map_tables %}, cannot be used in the primary key and in the columns that form the secondary index key{% endif %}
    ||
|| `Yson` |
[YSON](yson.md) in text or binary representation |
Does not support comparison{% if feature_map_tables %}, cannot be used in the primary key and in the columns that form the secondary index key{% endif %}
    ||
|| `Uuid` |
Universal identifier [UUID](https://tools.ietf.org/html/rfc4122) | Not supported in columnar tables
    ||
|#
{% note info "Size limitations" %}

The maximum size of a value in a cell {% if feature_map_tables %} of a non-key column {% endif %} with any string data type is 8 MB.

{% endnote %}
In contrast to the `Json` data type, which stores the original text representation provided by the user, `JsonDocument` uses a binary indexed representation. An important semantic difference is that `JsonDocument` does not preserve formatting, the order of keys in objects, or their duplicates.

Thanks to the indexed representation, `JsonDocument` allows you to traverse the document model using `JsonPath` without the need to parse the entire content. This enables efficient execution of operations from the [JSON API](../builtins/json.md), reducing latency and the cost of user queries. Query execution on `JsonDocument` can be several times more efficient, depending on the type of load.

Due to the added redundancy, `JsonDocument` is less efficient in terms of storage. The additional storage overhead depends on the specific content and averages 20–30% of the original volume. Storing data in `JsonDocument` format requires additional conversion from the text representation, which makes writing less efficient. However, for most read-intensive scenarios involving JSON data processing, this data type is preferable and is recommended for use.
{% note warning %}

The Double type is used to store numbers (JSON Number) in JsonDocument and for arithmetic operations on them in the [JSON API](../builtins/json.md). Loss of precision is possible when using non-standard number representations in the original JSON document.

{% endnote %}
## Date and time {#datetime}

#|
|| Type | Description | Possible values | Size (bytes) | Notes ||

||
`Date`
|
UTC midnight, accuracy to the day
|
from 00:00 January 1, 1970 to 00:00 January 1, 2106
|
2
|
—
||

||
`Date32`
|
UTC midnight, accuracy to the day
|
from 00:00 January 1, 144169 BC to 00:00 January 1, 148107 AD
|
4
|
—
||

||
`Datetime`
|
UTC time, accuracy to the second
|
from 00:00 January 1, 1970 to 00:00 January 1, 2106
|
4
|
—
||

||
`Datetime64`
|
UTC time, accuracy to the second
|
from 00:00 January 1, 144169 BC to 00:00 January 1, 148107 AD
|
8
|
—
||

||
`Timestamp`
|
UTC time, accuracy to the microsecond
|
from 00:00 January 1, 1970 to 00:00 January 1, 2106
|
8
|
—
||

||
`Timestamp64`
|
UTC time, accuracy to the microsecond
|
from 00:00 January 1, 144169 BC to 00:00 January 1, 148107 AD
|
8
|
—
||

||
`Interval`
|
Time interval, accuracy to the microsecond
|
from -136 years to +136 years
|
8
|
Not supported in columnar tables
||

||
`Interval64`
|
Time interval, accuracy to the microsecond
|
from -292277 years to +292277 years
|
8
|
Not supported in columnar tables
||

||
`TzDate`
|
UTC time corresponding to midnight in a given timezone
|
from 00:00 January 1, 1970 to 00:00 January 1, 2106
|

|
Not supported in table columns
||

||
`TzDate32`
|
UTC time corresponding to midnight in a given timezone
|
from 00:00 January 1, 144169 BC to 00:00 January 1, 148107 AD
|
4 and timezone mark
|
—
||

||
`TzDateTime`
|
UTC time with timezone mark and accuracy to the second
|
from 00:00 January 1, 1970 to 00:00 January 1, 2106
|

|
Not supported in table columns
||

||
`TzDateTime64`
|
UTC time with timezone mark and accuracy to the second
|
from 00:00 January 1, 144169 BC to 00:00 January 1, 148107 AD
|
8 and timezone mark
|
—
||

||
`TzTimestamp`
|
UTC time with timezone mark and accuracy to the microsecond
|
from 00:00 January 1, 1970 to 00:00 January 1, 2106
|

|
Not supported in table columns
||

||
`TzTimestamp64`
|
UTC time with timezone mark and accuracy to the microsecond
|
from 00:00 January 1, 144169 BC to 00:00 January 1, 148107 AD
|
8 and timezone mark
|
—
||
|#

<sup>1</sup> Midnight is understood as the moment when all _time_ components are equal to zero.
### Interval type behavior and limitations {#interval-type-behavior-and-limitations}

The `Interval` type uses syntax based on the [ISO 8601 standard](https://en.wikipedia.org/wiki/ISO_8601), with several specifics:

* only intervals that can be represented as an integer number of microseconds are supported (intervals in years `Y` and months `M` are not supported due to their variable duration);
* intervals with start/end points or repetitions are not supported;
* the use of a negative sign to indicate a shift into the past is supported;
* combining the week literal `W` with other literals is supported;
* microseconds can be specified as a fractional part of a second.

#### Interval data examples {#interval-data-examples}
```yql
SELECT
    Interval("P1W2DT2H3M4.567890S"), -- interval 1 week 2 days 2 hours 3 minutes 4.567890 seconds
    Interval("P1W"),                 -- interval 1 week (7 days)
    Interval("-P1D");                -- interval in the past 1 day (24 hours)
```
### Time zone label support features

The time zone label for the `TzDate`, `TzDatetime`, `TzTimestamp` types is an attribute that is used:

* When converting (using [CAST](../syntax/expressions.md#cast), [DateTime::Parse](../udf/list/datetime.md#parse), [DateTime::Format](../udf/list/datetime.md#format)) to and from a string.
* In [DateTime::Split](../udf/list/datetime.md#split) — the time zone component appears in `Resource<TM>`.

The actual time position value for these types is stored in UTC, and the time zone label does not participate in other calculations. For example:
```yql
SELECT -- these expressions are always true for any time zones: the time zone does not affect the point in time.
    AddTimezone(CurrentUtcDate(), "Europe/Moscow") ==
        AddTimezone(CurrentUtcDate(), "America/New_York"),
    AddTimezone(CurrentUtcDatetime(), "Europe/Moscow") ==
        AddTimezone(CurrentUtcDatetime(), "America/New_York");
```
It is important to understand that when converting between `TzDate` and `TzDatetime` or `TzTimestamp`, the date corresponds not to midnight local time in the timezone, but to midnight UTC for the date in UTC.
## Casting of primitive data types {#cast}
### Explicit casting {#explicit-cast}

Explicit casting using [CAST](../syntax/expressions.md#cast):

#### Casting to numeric types
| Type | Bool | Int8 | Int16 | Int32 | Int64 | Uint8 | Uint16 | Uint32 | Uint64 | Float | Double | Decimal |
| --------------- | -------------- | -------------- | -------------- | -------------- | -------------- | ---------------- | ---------------- | ---------------- | ---------------- | -------------- | -------------- | ------- |
| **Bool** | — | Yes<sup>1</sup> | Yes<sup>1</sup> | Yes<sup>1</sup> | Yes<sup>1</sup> | Yes<sup>1</sup> | Yes<sup>1</sup> | Yes<sup>1</sup> | Yes<sup>1</sup> | Yes<sup>1</sup> | Yes<sup>1</sup> | No |
| **Int8** | Yes<sup>2</sup> | — | Yes | Yes | Yes | Yes<sup>3</sup> | Yes<sup>3</sup> | Yes<sup>3</sup> | Yes<sup>3</sup> | Yes | Yes | Yes |
| **Int16** | Yes<sup>2</sup> | Yes<sup>4</sup> | — | Yes | Yes | Yes<sup>3,4</sup> | Yes<sup>3</sup> | Yes<sup>3</sup> | Yes<sup>3</sup> | Yes | Yes | Yes |
| **Int32** | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | — | Yes | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3</sup> | Yes<sup>3</sup> | Yes | Yes | Yes |
| **Int64** | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | — | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3</sup> | Yes | Yes | Yes |
| **Uint8** | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes | Yes | Yes | — | Yes | Yes | Yes | Yes | Yes | Yes |
| **Uint16** | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes | Yes | Yes<sup>4</sup> | — | Yes | Yes | Yes | Yes | Yes |
| **Uint32** | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes | Yes<sup>4</sup> | Yes<sup>4</sup> | — | Yes | Yes | Yes | Yes |
| **Uint64** | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | — | Yes | Yes | Yes |
| **Float** | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | — | Yes | No |
| **Double** | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes | — | No |
| **Decimal** | No | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | — |
| **String** | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| **Bytes** | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| **Utf8** | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| **Text** | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| **Json** | No | No | No | No | No | No | No | No | No | No | No | No |
| **Yson** | Yes<sup>5</sup> | Yes<sup>5</sup> | Yes<sup>5</sup> | Yes<sup>5</sup> | Yes<sup>5</sup> | Yes<sup>5</sup> | Yes<sup>5</sup> | Yes<sup>5</sup> | Yes<sup>5</sup> | Yes<sup>5</sup> | Yes<sup>5</sup> | No |
| **Uuid** | No | No | No | No | No | No | No | No | No | No | No | No |
| **Date** | No | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes | Yes | Yes<sup>4</sup> | Yes | Yes | Yes | Yes | Yes | No |
| **Datetime** | No | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes | Yes | Yes | Yes | No |
| **Timestamp** | No | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes | Yes | Yes | No |
| **Interval** | No | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3</sup> | Yes | Yes | No |
| **Date32** | No | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes | Yes | Yes<sup>4</sup> | Yes | Yes | Yes | Yes | Yes | No |
| **Datetime64** | No | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes | Yes | Yes | Yes | No |
| **Timestamp64** | No | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes | Yes | Yes | No |
| **Interval64** | No | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3</sup> | Yes | Yes | No |
<sup>1</sup> `True` is converted to `1`, `False` is converted to `0`.
<sup>2</sup> Any value other than `0` is converted to `True`, `0` is converted to `False`.
<sup>3</sup> Only possible if the value is non-negative.
<sup>4</sup> Only possible if the value falls within the range of allowed values.
<sup>5</sup> Using the built-in function [Yson::ConvertTo](../udf/list/yson.md#ysonconvertto).

#### Casting to date and time data types
| Type             | Date | Datetime | Timestamp | Interval | Date32 | Datetime64 | Timestamp64 | Interval64 |
| --------------- | ---- | -------- | --------- | -------- | ------ | ---------- | ----------- | ---------- |
| **Bool**        | No   | No       | No       | No       | No     | No        | No         | No        |
| **Int8**        | Yes  | Yes      | Yes      | Yes     | Yes    | Yes       | Yes        | Yes      |
| **Int16**       | Yes  | Yes      | Yes      | Yes     | Yes    | Yes       | Yes        | Yes      |
| **Int32**       | Yes  | Yes      | Yes      | Yes     | Yes    | Yes       | Yes        | Yes      |
| **Int64**       | Yes  | Yes      | Yes      | Yes     | Yes    | Yes       | Yes        | Yes      |
| **Uint8**       | Yes  | Yes      | Yes      | Yes     | Yes    | Yes       | Yes        | Yes      |
| **Uint16**      | Yes  | Yes      | Yes      | Yes     | Yes    | Yes       | Yes        | Yes      |
| **Uint32**      | Yes  | Yes      | Yes      | Yes     | Yes    | Yes       | Yes        | Yes      |
| **Uint64**      | Yes  | Yes      | Yes      | Yes     | Yes    | Yes       | Yes        | Yes      |
| **Float**       | No   | No      | No       | No       | No     | No        | No         | No        |
| **Double**      | No   | No      | No       | No       | No     | No        | No         | No        |
| **Decimal**     | No   | No      | No       | No       | No     | No        | No         | No        |
| **String**      | Yes  | Yes      | Yes      | Yes     | Yes    | Yes       | Yes        | Yes      |
| **Bytes**       | Yes  | Yes      | Yes      | Yes     | Yes    | Yes       | Yes        | Yes      |
| **Utf8**        | Yes  | Yes      | Yes      | Yes     | Yes    | Yes       | Yes        | Yes      |
| **Text**        | Yes  | Yes      | Yes      | Yes     | Yes    | Yes       | Yes        | Yes      |
| **Json**        | No   | No      | No       | No       | No     | No        | No         | No        |
| **Yson**        | No   | No      | No       | No       | No     | No        | No         | No        |
| **Uuid**        | No   | No      | No       | No       | No     | No        | No         | No        |
| **Date**        | —    | Yes      | Yes      | No       | Yes    | Yes       | Yes        | No       |
| **Datetime**    | Yes  | —        | Yes      | No       | Yes    | Yes       | Yes        | No       |
| **Timestamp**   | Yes  | Yes      | —        | No       | Yes    | Yes       | Yes        | No       |
| **Interval**    | No   | No      | No       | —       | No     | No        | No         | Yes      |
| **Date32**      | Yes  | Yes      | Yes      | No       | —      | Yes       | Yes        | No       |
| **Datetime64**  | Yes  | Yes      | Yes      | No       | Yes    | —         | Yes        | No       |
| **Timestamp64** | Yes  | Yes      | Yes      | No       | Yes    | Yes       | —         | No       |
| **Interval64**  | No   | No      | No       | Yes     | No     | No        | No         | —        |
#### Casting to other data types
| Type | String | Bytes | Utf8 | Text | Json | Yson | Uuid |
| --------------- | -------------- | -------------- | ---- | ---- | ---- | ---- | ---- |
| **Bool** | Yes | Yes | No | No | No | No | No |
| **Int8** | Yes | Yes | No | No | No | No | No |
| **Int16** | Yes | Yes | No | No | No | No | No |
| **Int32** | Yes | Yes | No | No | No | No | No |
| **Int64** | Yes | Yes | No | No | No | No | No |
| **Uint8** | Yes | Yes | No | No | No | No | No |
| **Uint16** | Yes | Yes | No | No | No | No | No |
| **Uint32** | Yes | Yes | No | No | No | No | No |
| **Uint64** | Yes | Yes | No | No | No | No | No |
| **Float** | Yes | Yes | No | No | No | No | No |
| **Double** | Yes | Yes | No | No | No | No | No |
| **Decimal** | Yes | Yes | No | No | No | No | No |
| **String** | — | — | Yes | Yes | Yes | Yes | Yes |
| **Bytes** | — | — | Yes | Yes | Yes | Yes | Yes |
| **Utf8** | Yes | Yes | — | — | No | No | No |
| **Text** | Yes | Yes | — | — | No | No | No |
| **Json** | Yes | Yes | Yes | Yes | — | No | No |
| **Yson** | Yes<sup>1</sup> | Yes<sup>1</sup> | No | No | No | No | No |
| **Uuid** | Yes | Yes | Yes | Yes | No | No | — |
| **Date** | Yes | Yes | Yes | Yes | No | No | No |
| **Datetime** | Yes | Yes | Yes | Yes | No | No | No |
| **Timestamp** | Yes | Yes | Yes | Yes | No | No | No |
| **Interval** | Yes | Yes | Yes | Yes | No | No | No |
| **Date32** | Yes | Yes | Yes | Yes | No | No | No |
| **Datetime64** | Yes | Yes | Yes | Yes | No | No | No |
| **Timestamp64** | Yes | Yes | Yes | Yes | No | No | No |
| **Interval64** | Yes | Yes | Yes | Yes | No | No | No |
<sup>1</sup> Using the built-in function [Yson::ConvertTo](../udf/list/yson.md#ysonconvertto).

##### Examples

{% include [x](../_includes/cast_examples.md) %}
### Implicit casting {#implicit-cast}

Implicit type casting that occurs in basic operations (`+`, `-`, `*`, `/`, `%`) between different data types. The table cells indicate the result type of the operation if it is possible:

#### Numeric types

If the numeric types do not match, a [BITCAST](../syntax/expressions.md#bitcast) of both arguments to the result type is performed first, and then the operation.
| Type | Int8 | Int16 | Int32 | Int64 | Uint8 | Uint16 | Uint32 | Uint64 | Float | Double |
| -------- | -------- | -------- | -------- | -------- | -------- | -------- | -------- | -------- | -------- | -------- |
| **Int8** | — | `Int16` | `Int32` | `Int64` | `Int8` | `Uint16` | `Uint32` | `Uint64` | `Float` | `Double` |
| **Int16** | `Int16` | — | `Int32` | `Int64` | `Int16` | `Int16` | `Uint32` | `Uint64` | `Float` | `Double` |
| **Int32** | `Int32` | `Int32` | — | `Int64` | `Int32` | `Int32` | `Int32` | `Uint64` | `Float` | `Double` |
| **Int64** | `Int64` | `Int64` | `Int64` | — | `Int64` | `Int64` | `Int64` | `Int64` | `Float` | `Double` |
| **Uint8** | `Int8` | `Int16` | `Int32` | `Int64` | — | `Uint16` | `Uint32` | `Uint64` | `Float` | `Double` |
| **Uint16** | `Uint16` | `Int16` | `Int32` | `Int64` | `Uint16` | — | `Uint32` | `Uint64` | `Float` | `Double` |
| **Uint32** | `Uint32` | `Uint32` | `Int32` | `Int64` | `Uint32` | `Uint32` | — | `Uint64` | `Float` | `Double` |
| **Uint64** | `Uint64` | `Uint64` | `Uint64` | `Int64` | `Uint64` | `Uint64` | `Uint64` | — | `Float` | `Double` |
| **Float** | `Float` | `Float` | `Float` | `Float` | `Float` | `Float` | `Float` | `Float` | — | `Double` |
| **Double** | `Double` | `Double` | `Double` | `Double` | `Double` | `Double` | `Double` | `Double` | `Double` | — |
#### Date and time types
| Type | Date | Datetime | Timestamp | Interval | TzDate | TzDatetime | TzTimestamp | Date32 | Datetime64 | Timestamp64 | Interval64 | TzDate32 | TzDatetime64 | TzTimestamp64 |
| ----------------- | ---- | ---------- | ----------- | -------- | -------- | ------------ | ------------- | -------- | ------------ | ------------- | ---------- | ---------- | -------------- | --------------- |
| **Date** | — | `DateTime` | `Timestamp` | — | `TzDate` | `TzDatetime` | `TzTimestamp` | `Date32` | `DateTime64` | `Timestamp64` | — | `TzDate32` | `TzDatetime64` | `TzTimestamp64` |
| **Datetime** | — | — | `Timestamp` | — | — | `TzDatetime` | `TzTimestamp` | — | `Datetime64` | `Timestamp64` | — | — | `TzDatetime64` | `TzTimestamp64` |
| **Timestamp** | — | — | — | — | — | — | `TzTimestamp` | — | — | `Timestamp64` | — | — | — | `TzTimestamp64` |
| **Interval** | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| **TzDate** | — | — | — | — | — | `TzDatetime` | `TzTimestamp` | — | — | — | — | `TzDate32` | `TzDatetime64` | `TzTimestamp64` |
| **TzDatetime** | — | — | — | — | — | — | `TzTimestamp` | — | — | — | — | — | `TzDatetime64` | `TzTimestamp64` |
| **TzTimestamp** | — | — | — | — | — | — | — | — | — | — | — | — | — | `TzTimestamp64` |
| **Date32** | — | — | — | — | — | — | — | — | `DateTime64` | `Timestamp64` | — | `TzDate32` | `TzDatetime64` | `TzTimestamp64` |
| **Datetime64** | — | — | — | — | — | — | — | — | — | `Timestamp64` | — | — | `TzDatetime64` | `TzTimestamp64` |
| **Timestamp64** | — | — | — | — | — | — | — | — | — | — | — | — | — | `TzTimestamp64` |
| **Interval64** | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
| **TzDate32** | — | — | — | — | — | — | — | — | — | — | — | — | `TzDatetime64` | `TzTimestamp64` |
| **TzDatetime64** | — | — | — | — | — | — | — | — | — | — | — | — | — | `TzTimestamp64` |
| **TzTimestamp64** | — | — | — | — | — | — | — | — | — | — | — | — | — | — |
