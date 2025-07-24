# Primitive data types

<!-- markdownlint-disable blanks-around-fences -->

The terms "simple", "primitive", and "elementary" data types are used synonymously.

## Numeric types {#numeric}

| Type | Description | Notes |
| ----- | ----- | ----- |
| `Bool` | Boolean value. | — |
| `Int8` | A signed integer.<br/>Acceptable values: from -2<sup>7</sup> to 2<sup>7</sup>–1. | — |
| `Int16` | A signed integer.<br/>Acceptable values: from –2<sup>15</sup> to 2<sup>15</sup>–1. | — |
| `Int32` | A signed integer.<br/>Acceptable values: from –2<sup>31</sup> to 2<sup>31</sup>–1. | — |
| `Int64` | A signed integer.<br/>Acceptable values: from –2<sup>63</sup> to 2<sup>63</sup>–1. | — |
| `Uint8` | An unsigned integer.<br/>Acceptable values: from 0 to 2<sup>8</sup>–1. | — |
| `Uint16` | An unsigned integer.<br/>Acceptable values: from 0 to 2<sup>16</sup>–1. | — |
| `Uint32` | An unsigned integer.<br/>Acceptable values: from 0 to 2<sup>32</sup>–1. | — |
| `Uint64` | An unsigned integer.<br/>Acceptable values: from 0 to 2<sup>64</sup>–1. | — |
| `Float` | A real number with variable precision, 4 bytes in size. | {% if feature_map_tables %}Can't be used in the primary key or in columns that form the key of a secondary index{% endif %} |
| `Double` | A real number with variable precision, 8 bytes in size. | {% if feature_map_tables %}Can't be used in the primary key or in columns that form the key of a secondary index{% endif %} |
| `Decimal(precision, scale)` | A real number with the specified precision, 16 bytes in size. Precision is the maximum total number of decimal digits stored and can range from 1 to 35. Scale is the maximum number of decimal digits stored to the right of the decimal point and can range from 0 to the precision value. | — |
{% if feature_map_tables %}
|`DyNumber` | A binary representation of a real number with an accuracy of up to 38 digits.<br/>Acceptable values: positive numbers from 1×10<sup>-130</sup> up to 1×10<sup>126</sup>–1, negative numbers from -1×10<sup>126</sup>–1 to -1×10<sup>-130</sup>, and 0.<br/>Compatible with the `Number` type in AWS DynamoDB. It's not recommended for {{ backend_name_lower }}-native applications. | — |
{% endif %}


## String types {#string}

| Type | Description | Notes |
| ----- | ----- | ----- |
| `String` | A string that can contain any binary data | — |
| `Utf8` | Text encoded in [UTF-8](https://en.wikipedia.org/wiki/UTF-8) | — |
| `Json` | [JSON](https://en.wikipedia.org/wiki/JSON) represented as text | Doesn't support matching{% if feature_map_tables %}, can't be used in the primary key or in columns that form the key of a secondary index{% endif %} |
| `JsonDocument` | [JSON](https://en.wikipedia.org/wiki/JSON) in an indexed binary representation | Doesn't support matching{% if feature_map_tables %}, can't be used in the primary key or in columns that form the key of a secondary index{% endif %} |
| `Yson` | [YSON](../udf/list/yson.md) in a textual or binary representation. | Doesn't support matching{% if feature_map_tables %}, can't be used in the primary key or in columns that form the key of a secondary index{% endif %} |
| `Uuid` | Universally unique identifier [UUID](https://tools.ietf.org/html/rfc4122) | — |

{% note info "Cell size restrictions" %}

The maximum value size for a {% if feature_map_tables %}non-key {% endif %} column cell with any string data type is 8 MB.

{% endnote %}

Unlike the `JSON` data type that stores the original text representation passed by the user, `JsonDocument` uses an indexed binary representation. An important difference from the point of view of semantics is that `JsonDocument` doesn't preserve formatting, the order of keys in objects, or their duplicates.

Thanks to the indexed view, `JsonDocument` lets you bypass the document model using `JsonPath` without the need to parse the full content. This helps efficiently perform operations from the [JSON API](../builtins/json.md), reducing delays and cost of user queries. Execution of `JsonDocument` queries can be up to several times more efficient depending on the type of load.

Due to the added redundancy, `JsonDocument` is less effective in storage. The additional storage overhead depends on the specific content, but is 20-30% of the original volume on average. Saving data in `JsonDocument` format requires additional conversion from the textual representation, which makes writing it less efficient. However, for most read-intensive scenarios that involve processing data from JSON, this data type is preferred and recommended.

{% note warning %}

To store numbers (JSON Number) in `JsonDocument`, as well as for arithmetic operations on them in the [JSON API](../builtins/json.md), the [Double](https://en.wikipedia.org/wiki/Double-precision_floating-point_format) type is used. Precision might be lost when non-standard representations of numbers are used in the source JSON document.

{% endnote %}



## Date and time {#datetime}

| Type | Description | Notes |
| ----- | ----- | ----- |
| `Date` | Date, precision to the day | Range of values for all time types except `Interval`: From 00:00 01.01.1970 to 00:00 01.01.2106. Internal `Date` representation: Unsigned 16-bit integer |
| `Datetime` | Date/time, precision to the second | Internal representation: Unsigned 32-bit integer |
| `Timestamp` | Date/time, precision to the microsecond | Internal representation: Unsigned 64-bit integer |
| `Interval` | Time interval (signed), precision to microseconds | Value range: From -136 years to +136 years. Internal representation: Signed 64-bit integer. {% if feature_map_tables %}Can't be used in the primary key or in columns that form the key of a secondary index{% endif %} |
| `TzDate` | Date with time zone label, precision to the day | Not supported in table columns |
| `TzDateTime` | Date/time with time zone label, precision to the second | Not supported in table columns |
| `TzTimestamp` | Date/time with time zone label, precision to the microsecond | Not supported in table columns |



### Supporting types with a time zone label

Time zone label for the `TzDate`, `TzDatetime`, `TzTimestamp` types is an attribute that is used:

* When converting ([CAST](../syntax/expressions.md#cast), [DateTime::Parse](../udf/list/datetime.md#parse), [DateTime::Format](../udf/list/datetime.md#format)) to a string and from a string.
* In [DateTime::Split](../udf/list/datetime.md#split), a timezone component is added to `Resource<TM>`.

The point in time for these types is stored in UTC, and the timezone label doesn't participate in any other calculations in any way. For example:

```yql
SELECT -- these expressions are always true for any timezones: the timezone doesn't affect the point in time.
    AddTimezone(CurrentUtcDate(), "Europe/Moscow") ==
        AddTimezone(CurrentUtcDate(), "America/New_York"),
    AddTimezone(CurrentUtcDatetime(), "Europe/Moscow") ==
        AddTimezone(CurrentUtcDatetime(), "America/New_York");
```

Keep in mind that when converting between `TzDate` and `TzDatetime`, or `TzTimestamp` the date's midnight doesn't follow the local time zone, but midnight in UTC for the date in UTC.



## Casting between data types {#cast}

### Explicit casting {#explicit-cast}

Explicit casting using [CAST](../syntax/expressions.md#cast):

#### Casting to numeric types

| Type          | Bool            | Int8            | Int16           | Int32           | Int64           | Uint8             | Uint16            | Uint32            | Uint64            | Float           | Double          | Decimal |
|---------------|-----------------|-----------------|-----------------|-----------------|-----------------|-------------------|-------------------|-------------------|-------------------|-----------------|-----------------|---------|
| **Bool**      | —               | Yes<sup>1</sup> | Yes<sup>1</sup> | Yes<sup>1</sup> | Yes<sup>1</sup> | Yes<sup>1</sup>   | Yes<sup>1</sup>   | Yes<sup>1</sup>   | Yes<sup>1</sup>   | Yes<sup>1</sup> | Yes<sup>1</sup> | No      |
| **Int8**      | Yes<sup>2</sup> | —               | Yes             | Yes             | Yes             | Yes<sup>3</sup>   | Yes<sup>3</sup>   | Yes<sup>3</sup>   | Yes<sup>3</sup>   | Yes             | Yes             | Yes     |
| **Int16**     | Yes<sup>2</sup> | Yes<sup>4</sup> | —               | Yes             | Yes             | Yes<sup>3,4</sup> | Yes<sup>3</sup>   | Yes<sup>3</sup>   | Yes<sup>3</sup>   | Yes             | Yes             | Yes     |
| **Int32**     | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | —               | Yes             | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3</sup>   | Yes<sup>3</sup>   | Yes             | Yes             | Yes     |
| **Int64**     | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | —               | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3</sup>   | Yes             | Yes             | Yes     |
| **Uint8**     | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes             | Yes             | Yes             | —                 | Yes               | Yes               | Yes               | Yes             | Yes             | Yes     |
| **Uint16**    | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes             | Yes             | Yes<sup>4</sup>   | —                 | Yes               | Yes               | Yes             | Yes             | Yes     |
| **Uint32**    | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes             | Yes<sup>4</sup>   | Yes<sup>4</sup>   | —                 | Yes               | Yes             | Yes             | Yes     |
| **Uint64**    | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup>   | Yes<sup>4</sup>   | Yes<sup>4</sup>   | —                 | Yes             | Yes             | Yes     |
| **Float**     | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | —               | Yes             | No      |
| **Double**    | Yes<sup>2</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes             | —               | No      |
| **Decimal**   | No              | Yes             | Yes             | Yes             | Yes             | Yes               | Yes               | Yes               | Yes               | Yes             | Yes             | —       |
| **String**    | Yes             | Yes             | Yes             | Yes             | Yes             | Yes               | Yes               | Yes               | Yes               | Yes             | Yes             | Yes     |
| **Utf8**      | Yes             | Yes             | Yes             | Yes             | Yes             | Yes               | Yes               | Yes               | Yes               | Yes             | Yes             | Yes     |
| **Json**      | No              | No              | No              | No              | No              | No                | No                | No                | No                | No              | No              | No      |
| **Yson**      | Yes<sup>5</sup> | Yes<sup>5</sup> | Yes<sup>5</sup> | Yes<sup>5</sup> | Yes<sup>5</sup> | Yes<sup>5</sup>   | Yes<sup>5</sup>   | Yes<sup>5</sup>   | Yes<sup>5</sup>   | Yes<sup>5</sup> | Yes<sup>5</sup> | No      |
| **Uuid**      | No              | No              | No              | No              | No              | No                | No                | No                | No                | No              | No              | No      |
| **Date**      | No              | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes             | Yes             | Yes<sup>4</sup>   | Yes               | Yes               | Yes               | Yes             | Yes             | No      |
| **Datetime**  | No              | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes             | Yes<sup>4</sup>   | Yes<sup>4</sup>   | Yes               | Yes               | Yes             | Yes             | No      |
| **Timestamp** | No              | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup>   | Yes<sup>4</sup>   | Yes<sup>4</sup>   | Yes               | Yes             | Yes             | No      |
| **Interval**  | No              | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes             | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3,4</sup> | Yes<sup>3</sup>   | Yes             | Yes             | No      |

<sup>1</sup> `True` is converted to `1` and `False` to `0`.
<sup>2</sup> Any value other than `0` is converted to `True`, `0` is converted to `False`.
<sup>3</sup> Possible only in case of a non-negative value.
<sup>4</sup> Possible only within the valid range.
<sup>5</sup> Using the built-in function [Yson::ConvertTo](../udf/list/yson.md#ysonconvertto).

#### Converting to date and time data types

| Type | Date | Datetime | Timestamp | Interval |
| --- | --- | --- | --- | --- |
| **Bool** | No | No | No | No |
| **INT** | Yes | Yes | Yes | Yes |
| **Uint** | Yes | Yes | Yes | Yes |
| **Float** | No | No | No | No |
| **Double** | No | No | No | No |
| **Decimal** | No | No | No | No |
| **String** | Yes | Yes | Yes | Yes |
| **Utf8** | Yes | Yes | Yes | Yes |
| **Json** | No | No | No | No |
| **Yson** | No | No | No | No |
| **Uuid** | No | No | No | No |
| **Date** | — | Yes | Yes | No |
| **Datetime** | Yes | — | Yes | No |
| **Timestamp** | Yes | Yes | — | No |
| **Interval** | No | No | No | — |

#### Conversion to other data types

| Type | String | Utf8 | Json | Yson | Uuid |
| --- | --- | --- | --- | --- | --- |
| **Bool** | Yes | No | No | No | No |
| **INT** | Yes | No | No | No | No |
| **Uint** | Yes | No | No | No | No |
| **Float** | Yes | No | No | No | No |
| **Double** | Yes | No | No | No | No |
| **Decimal** | Yes | No | No | No | No |
| **String** | — | Yes | Yes | Yes | Yes |
| **Utf8** | Yes | — | No | No | No |
| **Json** | Yes | Yes | — | No | No |
| **Yson** | Yes<sup>4</sup> | No | No | No | No |
| **Uuid** | Yes | Yes | No | No | — |
| **Date** | Yes | Yes | No | No | No |
| **Datetime** | Yes | Yes | No | No | No |
| **Timestamp** | Yes | Yes | No | No | No |
| **Interval** | Yes | Yes | No | No | No |

<sup>4</sup> Using the built-in function [Yson::ConvertTo](../udf/list/yson.md#ysonconvertto).

##### Examples

{% include [x](../_includes/cast_examples.md) %}

### Implicit casting {#implicit-cast}

Implicit type casting that occurs in basic operations ( +-\*/) between different data types. The table cells specify the operation result type, if the operation is possible:

#### Numeric types

| Type | Int | Uint | Float | Double |
| --- | --- | --- | --- | --- |
| **INT** | — | `INT` | `Float` | `Double` |
| **Uint** | `INT` | — | `Float` | `Double` |
| **Float** | `Float` | `Float` | — | `Double` |
| **Double** | `Double` | `Double` | `Double` | — |

#### Date and time types

| Type | Date | Datetime | Timestamp | Interval | TzDate | TzDatetime | TzTimestamp |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **Date** | — | — | — | `Date` | — | — | — |
| **Datetime** | — | — | — | `Datetime` | — | — | — |
| **Timestamp** | — | — | — | `Timestamp` | — | — | — |
| **Interval** | `Date` | `Datetime` | `Timestamp` | — | `TzDate` | `TzDatetime` | `TzTimestamp` |
| **TzDate** | — | — | — | `TzDate` | — | — | — |
| **TzDatetime** | — | — | — | `TzDatetime` | — | — | — |
| **TzTimestamp** | — | — | — | `TzTimestamp` | — | — | — |

