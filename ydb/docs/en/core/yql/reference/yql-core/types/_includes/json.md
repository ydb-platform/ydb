# Data representation in JSON format

## Bool {#bool}

Boolean value.

* Type in JSON: `bool`.
* Sample {{ backend_name }} value: `true`.
* Sample JSON value: `true`.

## Int8, Int16, Int32, Int64 {#int}

Signed integer types.

* Type in JSON: `number`.
* Sample {{ backend_name }} value: `123456`, `-123456`.
* Sample JSON value: `123456`, `-123456`.

## Uint8, Uint16, Uint32, Uint64 {#uint}

Unsigned integer types.

* Type in JSON: `number`.
* Sample {{ backend_name }} value: `123456`.
* Sample JSON value: `123456`.

## Float {#float}

Real 4-byte number.

* Type in JSON: `number`.
* Sample {{ backend_name }} value: `0.12345679`.
* Sample JSON value: `0.12345679`.

## Double {#double}

Real 8-byte number.

* Type in JSON: `number`.
* Sample {{ backend_name }} value: `0.12345678901234568`.
* Sample JSON value: `0.12345678901234568`.

## Decimal {#decimal}

Fixed-precision number. Only Decimal(22, 9) is supported.

* Type in JSON: `string`.
* Sample {{ backend_name }} value: `-320.789`.
* Sample JSON value: `"-320.789"`.

## String{% if audience != "external" %}, Yson{% endif %} {#string}

Binary strings. Encoding algorithm depending on the byte value:

* [0-31] — `\u00XX` (6 characters denoting the Unicode character code).
* [32-126] — as is. These are readable single-byte characters that don't need to be escaped.
* [127-255] — `\u00XX`.

Decoding is a reverse process. Character codes in `\u00XX`, maximum 255.

* Type in JSON: `string`.
* Sample {{ backend_name }} value: A sequence of 4 bytes:
  * 5 `0x05`: A control character.
  * 10 `0x0a`: The `\n` newline character.
  * 107 `0x6b`: The `k` character.
  * 255 `0xff`: The `ÿ` character in Unicode.
* Sample JSON value: `"\u0005\nk\u00FF"`.

## Utf8, Json, Uuid {#utf}

String types in UTF-8. Such strings are represented in JSON as strings with JSON characters escaped: `\\`, `\"`, `\n`, `\r`, `\t`, `\f`.

* Type in JSON: `string`.

* Sample {{ backend_name }} value: C++ code:

  ```c++
  "Escaped characters: "
  "\\ \" \f \b \t \r\n"
  "Non-escaped characters: "
  "/ ' < > & []() ".
  ```

* Sample JSON value: `"Escaped characters: \\ \" \f \b \t \r\nNon-escaped characters: / ' < > & []() "`.

## Date {#date}

Date. Uint64, unix time days.

* Type in JSON: `string`.
* Sample {{ backend_name }} value: `18367`.
* Sample JSON value: `"2020-04-15"`.

## Datetime {#datetime}

Date and time. Uint64, unix time seconds.

* Type in JSON: `string`.
* Sample {{ backend_name }} value: `1586966302`.
* Sample JSON value: `"2020-04-15T15:58:22Z"`.

## Timestamp {#timestamp}

Date and time. Uint64, unix time microseconds.

* Type in JSON: `string`.
* Sample {{ backend_name }} value: `1586966302504185`.
* Sample JSON value: `"2020-04-15T15:58:22.504185Z"`.

## Interval {#interval}

Time interval. Int64, precision to the microsecond, the interval values must not exceed 24 hours.

* Type in JSON: `number`.
* Sample {{ backend_name }} value: `123456`, `-123456`.
* Sample JSON value: `123456`, `-123456`.

## Optional {#optional}

Means that the value can be `null`. If the value is `null`, then in JSON it's also `null`. If the value is not `null`, then the JSON value is expressed as if the type isn't `Optional`.

* Type in JSON is missing.
* Sample {{ backend_name }} value: `null`.
* Sample JSON value: `null`.

## List {#list}

List. An ordered set of values of a given type.

* Type in JSON: `array`.
* Sample {{ backend_name }} value:
  * Type: `List<Int32>`.
  * Value: `1, 10, 100`.
* Sample JSON value: `[1,10,100]`.

## Stream {#stream}

Stream. Single-pass iterator by same-type values,

* Type in JSON: `array`.
* Sample {{ backend_name }} value:
  * Type: `Stream<Int32>`.
  * Value: `1, 10, 100`.
* Sample JSON value: `[1,10,100]`.

## Struct {#struct}

Structure. An unordered set of values with the specified names and type.

* Type in JSON: `object`.
* Sample {{ backend_name }} value:
  * Type: `Struct<'Id':Uint32,'Name':String,'Value':Int32,'Description':Utf8?>`;
  * Value: `"Id":1,"Name":"Anna","Value":-100,"Description":null`.
* Sample JSON value: `{"Id":1,"Name":"Anna","Value":-100,"Description":null}`.

## Tuple {#tuple}

Tuple. An ordered set of values of the set types.

* Type in JSON: `array`.
* Sample {{ backend_name }} value:
  * Type: `Tuple<Int32??,Int64???,String??,Utf8???>`;
  * Value: `10,-1,null,"Some string"`.
* Sample JSON value: `[10,-1,null,"Some string"]`.

## Dict {#dict}

Dictionary. An unordered set of key-value pairs. The type is set both for the key and the value. It's written in JSON to an array of arrays including two items.

* Type in JSON: `array`.
* Sample {{ backend_name }} value:
  * Type: `Dict<Int64,String>`.
  * Value: `1:"Value1",2:"Value2"`.
* Sample JSON value: `[[1,"Value1"],[2,"Value2"]]`.

