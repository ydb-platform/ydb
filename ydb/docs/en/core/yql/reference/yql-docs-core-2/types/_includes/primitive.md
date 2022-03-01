# Primitive data types

The terms "simple", "primitive", and "elementary" data types are used synonymously.

## Numeric types {#numeric}

{% include [datatypes](datatypes_primitive_number.md) %}

## String types {#string}

{% include [datatypes](datatypes_primitive_string.md) %}

## Date and time {#datetime}

{% include [datatypes](datatypes_primitive_datetime.md) %}

{% include [x](tz_date_types.md) %}

# Casting between data types {#cast}

## Explicit casting {#explicit-cast}

Explicit casting using [CAST](../../syntax/expressions.md#cast):

### Casting to numeric types

| Type | Bool | Int | Uint | Float | Double | Decimal |
| --- | --- | --- | --- | --- | --- | --- |
| **Bool** | — | Yes<sup>1</sup> | Yes<sup>1</sup> | Yes<sup>1</sup> | Yes<sup>1</sup> | No | Yes | No |
| **INT** | Yes<sup>2</sup> | — | Yes<sup>3</sup> | Yes | Yes | Yes |
| **Uint** | Yes<sup>2</sup> | Yes | — | Yes | Yes | Yes |
| **Float** | Yes<sup>2</sup> | Yes | Yes | — | Yes | No |
| **Double** | Yes<sup>2</sup> | Yes | Yes | Yes | — | No |
| **Decimal** | No | Yes | Yes | Yes | Yes | — |
| **String** | Yes | Yes | Yes | Yes | Yes | Yes |
| **Utf8** | Yes | Yes | Yes | Yes | Yes | Yes |
| **Json** | No | No | No | No | No | No |
| **Yson** | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> | Yes<sup>4</sup> |
| **Uuid** | No | No | No | No | No | No |
| **Date** | No | Yes | Yes | Yes | Yes | No | Yes |
| **Datetime** | No | Yes | Yes | Yes | Yes | No |
| **Timestamp** | No | Yes | Yes | Yes | Yes | No |
| **Interval** | No | Yes | Yes | Yes | Yes | No |

<sup>1</sup> `True` is converted to `1` and `False` to `0`.
<sup>2</sup> Any value other than `0` is converted to `True`, `0` is converted to `False`.
<sup>3</sup> Possible only in the case of a non-negative value.
<sup>4</sup> Using the built-in function [Yson::ConvertTo](../../udf/list/yson.md#ysonconvertto).

### Converting to date and time data types

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
| **Interval** | No | No | No | — | — |

### Conversion to other data types

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

<sup>4</sup> Using the built-in function [Yson::ConvertTo](../../udf/list/yson.md#ysonconvertto).

**Examples**

{% include [x](../../_includes/cast_examples.md) %}

## Implicit casting {#implicit-cast}

Implicit type casting that occurs in basic operations ( +-\*/) between different data types. The table cells specify the operation result type, if the operation is possible:

### Numeric types

| Type | Int | Uint | Float | Double |
| --- | --- | --- | --- | --- |
| **INT** | — | `INT` | `Float` | `Double` |
| **Uint** | `INT` | — | `Float` | `Double` |
| **Float** | `Float` | `Float` | — | `Double` |
| **Double** | `Double` | `Double` | `Double` | — |

### Date and time types

| Type | Date | Datetime | Timestamp | Interval | TzDate | TzDatetime | TzTimestamp |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **Date** | — | — | — | `Date` | — | — | — |
| **Datetime** | — | — | — | `Datetime` | — | — | — |
| **Timestamp** | — | — | — | `Timestamp` | — | — | — |
| **Interval** | `Date` | `Datetime` | `Timestamp` | — | `TzDate` | `TzDatetime` | `TzTimestamp` |
| **TzDate** | — | — | — | `TzDate` | — | — | — |
| **TzDatetime** | — | — | — | `TzDatetime` | — | — | — |
| **TzTimestamp** | — | — | — | `TzTimestamp` | — | — | — |

