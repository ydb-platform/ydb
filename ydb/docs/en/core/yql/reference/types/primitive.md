# Primitive data types

<!-- markdownlint-disable blanks-around-fences -->

The terms "simple", "primitive", and "elementary" data types are used synonymously.

## Numeric types {#numeric}

#|
|| Type |
Description |
Notes
    ||
|| `Bool` |
Boolean value |
    ||
|| `Int8` |
Signed integer
| Acceptable values: from –2<sup>7</sup> to 2<sup>7</sup>–1 |
    ||
|| `Int16` |
Signed integer
| Acceptable values: from –2<sup>15</sup> to 2<sup>15</sup>–1 |
    ||
|| `Int32` |
Signed integer
| Acceptable values: from –2<sup>31</sup> to 2<sup>31</sup>–1 |
    ||
|| `Int64` |
Signed integer
| Acceptable values: from –2<sup>63</sup> to 2<sup>63</sup>–1 |
    ||
|| `Uint8` |
Unsigned integer
| Acceptable values: from 0 to 2<sup>8</sup>–1 |
    ||
|| `Uint16` |
Unsigned integer
| Acceptable values: from 0 to 2<sup>16</sup>–1 |
    ||
|| `Uint32` |
Unsigned integer
| Acceptable values: from 0 to 2<sup>32</sup>–1 |
    ||
|| `Uint64` |
Unsigned integer
| Acceptable values: from 0 to 2<sup>64</sup>–1 |
    ||
|| `Float` |
Real number with variable precision, 4 bytes in size |
{% if feature_map_tables %}Can't be used in the primary key or in columns that form the key of a secondary index{% endif %}
    ||
|| `Double` |
Real number with variable precision, 8 bytes in size |
{% if feature_map_tables %}Can't be used in the primary key or in columns that form the key of a secondary index{% endif %}
    ||
|| `Decimal(precision, scale)` |
Real number with fixed precision, 16 bytes in size. Precision is the maximum total number of decimal digits stored, takes values from 1 to 35. Scale is the maximum number of decimal digits stored to the right of the decimal point, takes values from 0 to the precision value. |
{% if feature_map_tables %}Can't be used in the primary key or in columns that form the key of a secondary index{% endif %}
    ||
{% if feature_map_tables %}
|| `DyNumber` |
Binary representation of a real number with precision up to 38 digits.
| Acceptable values: positive from 1×10<sup>-130</sup> to 1×10<sup>126</sup>–1, negative from -1×10<sup>126</sup>–1 to -1×10<sup>-130</sup> and 0.
Compatible with the `Number` type in AWS DynamoDB. Not recommended for use in {{ backend_name_lower }}-native applications. Not available for column-oriented tables. |
    ||
{% endif %}
|#


## String types {#string}

#|
|| Type | Description | Notes ||
|| `String` |
String, can contain arbitrary binary data |
    ||
|| `Bytes` (alias for type `String` |
String, can contain arbitrary binary data |
    ||
|| `Utf8` |
Text in [UTF-8](https://en.wikipedia.org/wiki/UTF-8) encoding |
    ||
|| `Text` (alias for type `Utf8` |
Text in [UTF-8](https://en.wikipedia.org/wiki/UTF-8) encoding |
    ||
|| `Json` |
[JSON](https://en.wikipedia.org/wiki/JSON) in textual representation |
Doesn't support matching{% if feature_map_tables %}, can't be used in the primary key or in columns that form the key of a secondary index{% endif %}
    ||
|| `JsonDocument` |
[JSON](https://en.wikipedia.org/wiki/JSON) in binary indexed representation |
Doesn't support matching{% if feature_map_tables %}, can't be used in the primary key or in columns that form the key of a secondary index{% endif %}
    ||
|| `Yson` |
YSON in textual or binary representation |
Doesn't support matching{% if feature_map_tables %}, can't be used in the primary key or in columns that form the key of a secondary index{% endif %}
    ||
|| `Uuid` |
Universal identifier [UUID](https://tools.ietf.org/html/rfc4122) |
Not available for column-oriented tables
    ||
|#

{% note info "Size restrictions" %}

Maximum value size in a cell {% if feature_map_tables %} of a non-key column {% endif %} with any string data type — 8 MB.

{% endnote %}

Unlike the `JSON` data type that stores the original text representation passed by the user, `JsonDocument` uses an indexed binary representation. An important difference from the point of view of semantics is that `JsonDocument` doesn't preserve formatting, the order of keys in objects, or their duplicates.

Thanks to the indexed view, `JsonDocument` lets you bypass the document model using `JsonPath` without the need to parse the full content. This helps efficiently perform operations from the [JSON API](../builtins/json.md), reducing delays and cost of user queries. Execution of `JsonDocument` queries can be up to several times more efficient depending on the type of load.

Due to the added redundancy, `JsonDocument` is less effective in storage. The additional storage overhead depends on the specific content, but is 20-30% of the original volume on average. Saving data in `JsonDocument` format requires additional conversion from the textual representation, which makes writing it less efficient. However, for most read-intensive scenarios that involve processing data from JSON, this data type is preferred and recommended.

{% note warning %}

To store numbers (JSON Number) in `JsonDocument`, as well as for arithmetic operations on them in the [JSON API](../builtins/json.md), the [Double](https://en.wikipedia.org/wiki/Double-precision_floating-point_format) type is used. Precision might be lost when non-standard representations of numbers are used in the source JSON document.

{% endnote %}



## Date and time {#datetime}

#|
|| Type | Description | Possible values | Size (bytes) | Notes ||

||
`Date`
|
A moment in time corresponding to midnight<sup>1</sup> in UTC, precision to the day
|
from 00:00 01.01.1970 to 00:00 01.01.2106
|
2
|
—
||

||
`Date32`
|
A moment in time corresponding to midnight<sup>1</sup> in UTC, precision to the day
|
from 00:00 01.01.144169 BC to 00:00 01.01.148107 AD
|
4
|
—
||

||
`Datetime`
|
A moment in time in UTC, precision to the second
|
from 00:00 01.01.1970 to 00:00 01.01.2106
|
4
|
—
||

||
`Datetime64`
|
A moment in time in UTC, precision to the second
|
from 00:00 01.01.144169 BC to 00:00 01.01.148107 AD
|
8
|
—
||

||
`Timestamp`
|
A moment in time in UTC, precision to the microsecond
|
from 00:00 01.01.1970 to 00:00 01.01.2106
|
8
|
—
||

||
`Timestamp64`
|
A moment in time in UTC, precision to the microsecond
|
from 00:00 01.01.144169 BC to 00:00 01.01.148107 AD
|
8
|
—
||

||
`Interval`
|
Time interval, precision to the microsecond
|
from -136 years to +136 years
|
8
|
{% if feature_map_tables %}Can't be used in the primary key or in columns that form the key of a secondary index.{% else %}—{% endif %} Not available for column-oriented tables
||

||
`Interval64`
|
Time interval, precision to the microsecond
|
from -292277 years to +292277 years
|
8
|
Not available for column-oriented tables
||

||
`TzDate`
|
A moment in time in UTC corresponding to midnight in the specified timezone
|
from 00:00 01.01.1970 to 00:00 01.01.2106
|

|
Not supported in table columns
||

||
`TzDate32`
|
A moment in time in UTC corresponding to midnight in the specified timezone
|
from 00:00 01.01.144169 BC to 00:00 01.01.148107 AD
|
4 and timezone label
|
—
||

||
`TzDateTime`
|
A moment in time in UTC with timezone label and precision to the second
|
from 00:00 01.01.1970 to 00:00 01.01.2106
|

|
Not supported in table columns
||

||
`TzDateTime64`
|
A moment in time in UTC with timezone label and precision to the second
|
from 00:00 01.01.144169 BC to 00:00 01.01.148107 AD
|
8 and timezone label
|
—
||

||
`TzTimestamp`
|
A moment in time in UTC with timezone label and precision to the microsecond
|
from 00:00 01.01.1970 to 00:00 01.01.2106
|

|
Not supported in table columns
||

||
`TzTimestamp64`
|
A moment in time in UTC with timezone label and precision to the microsecond
|
from 00:00 01.01.144169 BC to 00:00 01.01.148107 AD
|
8 and timezone label
|
—
||
|#

<sup>1</sup> Midnight refers to the time point where all _time_ components equal zero.

### Features of supporting types with timezone label

Timezone label for the `TzDate`, `TzDatetime`, `TzTimestamp` types is an attribute that is used:

* When converting ([CAST](../syntax/expressions.md#cast), [DateTime::Parse](../udf/list/datetime.md#parse), [DateTime::Format](../udf/list/datetime.md#format)) to a string and from a string.
* In [DateTime::Split](../udf/list/datetime.md#split) - a timezone component appears in `Resource<TM>`.

The actual time position value for these types is stored in UTC, and the timezone label doesn't participate in other calculations in any way. For example:

```yql
SELECT --these expressions are always true for any timezones: timezone doesn't affect the point in time.
    AddTimezone(CurrentUtcDate(), "Europe/Moscow") ==
        AddTimezone(CurrentUtcDate(), "America/New_York"),
    AddTimezone(CurrentUtcDatetime(), "Europe/Moscow") ==
        AddTimezone(CurrentUtcDatetime(), "America/New_York");
```

It's important to understand that when converting between `TzDate` and `TzDatetime` or `TzTimestamp`, the date corresponds not to midnight in the local timezone time, but to midnight in UTC for the date in UTC.



## Simple data types casting {#cast}

### Explicit casting {#explicit-cast}

Explicit casting using [CAST](../syntax/expressions.md#cast):

#### Casting to numeric types

| Type             | Bool           | Int8           | Int16          | Int32          | Int64          | Uint8            | Uint16           | Uint32           | Uint64           | Float          | Double         | Decimal |
| --------------- | -------------- | -------------- | -------------- | -------------- | -------------- | ---------------- | ---------------- | ---------------- | ---------------- | -------------- | -------------- | ------- |
| **Bool**        | —              | Yes<sup>1</sup> | Yes<sup>1</sup> | Yes<sup>1</sup> | Yes<sup>1</sup> | Yes<sup>1</sup>   | Yes<sup>1</sup>   | Yes<sup>1</sup>   | Yes<sup>1</sup>   | Yes<sup>1</sup> | Yes<sup>1</sup> | No     |
| **Int8**        | Yes<sup>2</sup> | —              | Yes             | Yes             | Yes             | Yes<sup>3</sup>   | Yes<sup>3</sup>   | Yes<sup>3</sup>   | Yes<sup>3</sup>   | Yes             | Yes             | Yes      |
| **Int16**       | Yes<sup>2</sup> | Yes<sup>4</sup> | —              | Yes             | Yes             | Yes<sup>3,4</sup> | Yes<sup>3</sup>   | Yes<sup>3</sup>   | Yes<sup>3</sup>   | Yes             | Yes             | Yes      |
| **Int32**       | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | —              | Yes             | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3</sup>   | Yes<sup>3</sup>   | Yes             | Yes             | Yes      |
| **Int64**       | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | —              | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3</sup>   | Yes             | Yes             | Yes      |
| **Uint8**       | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes             | Yes             | Yes             | —                | Yes               | Yes               | Yes               | Yes             | Yes             | Yes      |
| **Uint16**      | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes             | Yes             | Yes<sup>4</sup>   | —                | Yes               | Yes               | Yes             | Yes             | Yes      |
| **Uint32**      | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes             | Yes<sup>4</sup>   | Yes<sup>4</sup>   | —                | Yes               | Yes             | Yes             | Yes      |
| **Uint64**      | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup>   | Yes<sup>4</sup>   | Yes<sup>4</sup>   | —                | Yes             | Yes             | Yes      |
| **Float**       | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | —              | Yes             | No     |
| **Double**      | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes             | —              | No     |
| **Decimal**     | No            | Yes             | Yes             | Yes             | Yes             | Yes               | Yes               | Yes               | Yes               | Yes             | Yes             | —       |
| **String**      | Yes             | Yes             | Yes             | Yes             | Yes             | Yes               | Yes               | Yes               | Yes               | Yes             | Yes             | Yes      |
| **Bytes**       | Yes             | Yes             | Yes             | Yes             | Yes             | Yes               | Yes               | Yes               | Yes               | Yes             | Yes             | Yes      |
| **Utf8**        | Yes             | Yes             | Yes             | Yes             | Yes             | Yes               | Yes               | Yes               | Yes               | Yes             | Yes             | Yes      |
| **Text**        | Yes             | Yes             | Yes             | Yes             | Yes             | Yes               | Yes               | Yes               | Yes               | Yes             | Yes             | Yes      |
| **Json**        | No              | No              | No              | No              | No              | No                | No                | No                | No                | No              | No              | No      |
| **Yson**        | Yes<sup>5</sup> | Yes<sup>5</sup> | Yes<sup>5</sup> | Yes<sup>5</sup> | Yes<sup>5</sup> | Yes<sup>5</sup>   | Yes<sup>5</sup>   | Yes<sup>5</sup>   | Yes<sup>5</sup>   | Yes<sup>5</sup> | Yes<sup>5</sup> | No      |
| **Uuid**        | No            | No              | No              | No              | No              | No                | No                | No                | No                | No              | No              | No     |
| **Date**        | No              | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes             | Yes             | Yes<sup>4</sup>   | Yes               | Yes               | Yes               | Yes             | Yes             | No      |
| **Datetime**    | No              | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes             | Yes<sup>4</sup>   | Yes<sup>4</sup>   | Yes               | Yes               | Yes             | Yes             | No      |
| **Timestamp**   | No              | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup>   | Yes<sup>4</sup>   | Yes<sup>4</sup>   | Yes               | Yes             | Yes             | No      |
| **Interval**    | No              | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes             | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3</sup>   | Yes             | Yes             | No      |
| **Date32**      | No              | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes             | Yes             | Yes<sup>4</sup>   | Yes               | Yes               | Yes               | Yes             | Yes             | No      |
| **Datetime64**  | No              | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes             | Yes<sup>4</sup>   | Yes<sup>4</sup>   | Yes               | Yes               | Yes             | Yes             | No      |
| **Timestamp64** | No              | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup>   | Yes<sup>4</sup>   | Yes<sup>4</sup>   | Yes               | Yes             | Yes             | No      |
| **Interval64**  | No              | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes             | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3</sup>   | Yes             | Yes             | No      |

<sup>1</sup> `True` is converted to `1`, `False` is converted to `0`.
<sup>2</sup> Any value other than `0` is converted to `True`, `0` is converted to `False`.
<sup>3</sup> Possible only in case of a non-negative value.
<sup>4</sup> Possible only in case of falling within the range of acceptable values.
<sup>5</sup> Using the built-in function [Yson::ConvertTo](../udf/list/yson.md#ysonconvertto).

#### Casting to date and time data types

| Type             | Date | Datetime | Timestamp | Interval | Date32 | Datetime64 | Timestamp64 | Interval64 |
| --------------- | ---- | -------- | --------- | -------- | ------ | ---------- | ----------- | ---------- |
| **Bool**        | No  | No      | No       | No      | No    | No        | No         | No        |
| **Int8**        | Yes   | Yes       | Yes        | Yes       | Yes     | Yes         | Yes          | Yes         |
| **Int16**       | Yes   | Yes       | Yes        | Yes       | Yes     | Yes         | Yes          | Yes         |
| **Int32**       | Yes   | Yes       | Yes        | Yes       | Yes     | Yes         | Yes          | Yes         |
| **Int64**       | Yes   | Yes       | Yes        | Yes       | Yes     | Yes         | Yes          | Yes         |
| **Uint8**       | Yes   | Yes       | Yes        | Yes       | Yes     | Yes         | Yes          | Yes         |
| **Uint16**      | Yes   | Yes       | Yes        | Yes       | Yes     | Yes         | Yes          | Yes         |
| **Uint32**      | Yes   | Yes       | Yes        | Yes       | Yes     | Yes         | Yes          | Yes         |
| **Uint64**      | Yes   | Yes       | Yes        | Yes       | Yes     | Yes         | Yes          | Yes         |
| **Float**       | No  | No      | No       | No      | No    | No        | No         | No        |
| **Double**      | No  | No      | No       | No      | No    | No        | No         | No        |
| **Decimal**     | No  | No      | No       | No      | No    | No        | No         | No        |
| **String**      | Yes   | Yes       | Yes        | Yes       | Yes     | Yes         | Yes          | Yes         |
| **Bytes**       | Yes   | Yes       | Yes        | Yes       | Yes     | Yes         | Yes          | Yes         |
| **Text**        | Yes   | Yes       | Yes        | Yes       | Yes     | Yes         | Yes          | Yes         |
| **Json**        | No  | No      | No       | No      | No    | No        | No         | No        |
| **Yson**        | No  | No      | No       | No      | No    | No        | No         | No        |
| **Uuid**        | No  | No      | No       | No      | No    | No        | No         | No        |
| **Date**        | —    | Yes       | Yes        | No      | Yes     | Yes         | Yes          | No        |
| **Datetime**    | Yes   | —        | Yes        | No      | Yes     | Yes         | Yes          | No        |
| **Timestamp**   | Yes   | Yes       | —         | No      | Yes     | Yes         | Yes          | No        |
| **Interval**    | No  | No      | No       | —        | No    | No        | No         | Yes         |
| **Date32**      | Yes   | Yes       | Yes        | No      | —      | Yes         | Yes          | No        |
| **Datetime64**  | Yes   | Yes       | Yes        | No      | Yes     | —          | Yes          | No        |
| **Timestamp64** | Yes   | Yes       | Yes        | No      | Yes     | Yes         | —           | No        |
| **Interval64**  | No  | No      | No       | Yes       | No    | No        | No         | —          |

#### Casting to other data types

| Type            | String         | Bytes          | Utf8 | Text | Json | Yson | Uuid |
| --------------- | -------------- | -------------- | ---- | ---- | ---- | ---- | ---- |
| **Bool**        | Yes            | Yes            | No   | No   | No   | No   | No   |
| **Int8**        | Yes            | Yes            | No   | No   | No   | No   | No   |
| **Int16**       | Yes            | Yes            | No   | No   | No   | No   | No   |
| **Int32**       | Yes            | Yes            | No   | No   | No   | No   | No   |
| **Int64**       | Yes            | Yes            | No   | No   | No   | No   | No   |
| **Uint8**       | Yes            | Yes            | No   | No   | No   | No   | No   |
| **Uint16**      | Yes            | Yes            | No   | No   | No   | No   | No   |
| **Uint32**      | Yes            | Yes            | No   | No   | No   | No   | No   |
| **Uint64**      | Yes            | Yes            | No   | No   | No   | No   | No   |
| **Float**       | Yes            | Yes            | No   | No   | No   | No   | No   |
| **Double**      | Yes            | Yes            | No   | No   | No   | No   | No   |
| **Decimal**     | Yes            | Yes            | No   | No   | No   | No   | No   |
| **String**      | —              | —              | Yes  | Yes  | Yes  | Yes  | Yes  |
| **Bytes**       | —              | —              | Yes  | Yes  | Yes  | Yes  | Yes  |
| **Utf8**        | Yes            | Yes            | —    | —    | No   | No   | No   |
| **Text**        | Yes            | Yes            | —    | —    | No   | No   | No   |
| **Json**        | Yes            | Yes            | Yes  | Yes  | —    | No   | No   |
| **Yson**        | Yes<sup>1</sup>| Yes<sup>1</sup>| No   | No   | No   | —    | No   |
| **Uuid**        | Yes            | Yes            | Yes  | Yes  | No   | No   | —    |
| **Date**        | Yes            | Yes            | Yes  | Yes  | No   | No   | No   |
| **Datetime**    | Yes            | Yes            | Yes  | Yes  | No   | No   | No   |
| **Timestamp**   | Yes            | Yes            | Yes  | Yes  | No   | No   | No   |
| **Interval**    | Yes            | Yes            | Yes  | Yes  | No   | No   | No   |
| **Date32**      | Yes            | Yes            | Yes  | Yes  | No   | No   | No   |
| **Datetime64**  | Yes            | Yes            | Yes  | Yes  | No   | No   | No   |
| **Timestamp64** | Yes            | Yes            | Yes  | Yes  | No   | No   | No   |
| **Interval64**  | Yes            | Yes            | Yes  | Yes  | No   | No   | No   |

<sup>1</sup> Using the built-in function [Yson::ConvertTo](../udf/list/yson.md#ysonconvertto).

##### Examples

{% include [x](../_includes/cast_examples.md) %}

### Implicit casting {#implicit-cast}

Implicit type casting that occurs in basic operations (`+`, `-`, `*`, `/`, `%`) between different data types. The table cells specify the operation result type, if the operation is possible:

#### Numeric types

When numeric types don't match, first BitCast of both arguments to the result type is performed, and then the operation.

| Type        | Int8     | Int16    | Int32    | Int64    | Uint8    | Uint16   | Uint32   | Uint64   | Float    | Double   |
| ---------- | -------- | -------- | -------- | -------- | -------- | -------- | -------- | -------- | -------- | -------- |
| **Int8**   | —        | `Int16`  | `Int32`  | `Int64`  | `Int8`   | `Uint16` | `Uint32` | `Uint64` | `Float`  | `Double` |
| **Int16**  | `Int16`  | —        | `Int32`  | `Int64`  | `Int16`  | `Int16`  | `Uint32` | `Uint64` | `Float`  | `Double` |
| **Int32**  | `Int32`  | `Int32`  | —        | `Int64`  | `Int32`  | `Int32`  | `Int32`  | `Uint64` | `Float`  | `Double` |
| **Int64**  | `Int64`  | `Int64`  | `Int64`  | —        | `Int64`  | `Int64`  | `Int64`  | `Int64`  | `Float`  | `Double` |
| **Uint8**  | `Int8`   | `Int16`  | `Int32`  | `Int64`  | —        | `Uint16` | `Uint32` | `Uint64` | `Float`  | `Double` |
| **Uint16** | `Uint16` | `Int16`  | `Int32`  | `Int64`  | `Uint16` | —        | `Uint32` | `Uint64` | `Float`  | `Double` |
| **Uint32** | `Uint32` | `Uint32` | `Int32`  | `Int64`  | `Uint32` | `Uint32` | —        | `Uint64` | `Float`  | `Double` |
| **Uint64** | `Uint64` | `Uint64` | `Uint64` | `Int64`  | `Uint64` | `Uint64` | `Uint64` | —        | `Float`  | `Double` |
| **Float**  | `Float`  | `Float`  | `Float`  | `Float`  | `Float`  | `Float`  | `Float`  | `Float`  | —        | `Double` |
| **Double** | `Double` | `Double` | `Double` | `Double` | `Double` | `Double` | `Double` | `Double` | `Double` | —        |

#### Date and time types

| Type               | Date | Datetime   | Timestamp   | Interval | TzDate   | TzDatetime   | TzTimestamp   | Date32   | Datetime64   | Timestamp64   | Interval64 | TzDate32   | TzDatetime64   | TzTimestamp64   |
| ----------------- | ---- | ---------- | ----------- | -------- | -------- | ------------ | ------------- | -------- | ------------ | ------------- | ---------- | ---------- | -------------- | --------------- |
| **Date**          | —    | `DateTime` | `Timestamp` | —        | `TzDate` | `TzDatetime` | `TzTimestamp` | `Date32` | `DateTime64` | `Timestamp64` | —          | `TzDate32` | `TzDatetime64` | `TzTimestamp64` |
| **Datetime**      | —    | —          | `Timestamp` | —        | —        | `TzDatetime` | `TzTimestamp` | —        | `Datetime64` | `Timestamp64` | —          | —          | `TzDatetime64` | `TzTimestamp64` |
| **Timestamp**     | —    | —          | —           | —        | —        | —            | `TzTimestamp` | —        | —            | `Timestamp64` | —          | —          | —              | `TzTimestamp64` |
| **Interval**      | —    | —          | —           | —        | —        | —            | —             | —        | —            | —             | —          | —          | —              | —               |
| **TzDate**        | —    | —          | —           | —        | —        | `TzDatetime` | `TzTimestamp` | —        | —            | —             | —          | `TzDate32` | `TzDatetime64` | `TzTimestamp64` |
| **TzDatetime**    | —    | —          | —           | —        | —        | —            | `TzTimestamp` | —        | —            | —             | —          | —          | `TzDatetime64` | `TzTimestamp64` |
| **TzTimestamp**   | —    | —          | —           | —        | —        | —            | —             | —        | —            | —             | —          | —          | —              | `TzTimestamp64` |
| **Date32**        | —    | —          | —           | —        | —        | —            | —             | —        | `DateTime64` | `Timestamp64` | —          | `TzDate32` | `TzDatetime64` | `TzTimestamp64` |
| **Datetime64**    | —    | —          | —           | —        | —        | —            | —             | —        | —            | `Timestamp64` | —          | —          | `TzDatetime64` | `TzTimestamp64` |
| **Timestamp64**   | —    | —          | —           | —        | —        | —            | —             | —        | —            | —             | —          | —          | —              | `TzTimestamp64` |
| **Interval64**    | —    | —          | —           | —        | —        | —            | —             | —        | —            | —             | —          | —          | —              | —               |
| **TzDate32**      | —    | —          | —           | —        | —        | —            | —             | —        | —            | —             | —          | —          | `TzDatetime64` | `TzTimestamp64` |
| **TzDatetime64**  | —    | —          | —           | —        | —        | —            | —             | —        | —            | —             | —          | —          | —              | `TzTimestamp64` |
| **TzTimestamp64** | —    | —          | —           | —        | —        | —            | —             | —        | —            | —             | —          | —          | —              | —               |
