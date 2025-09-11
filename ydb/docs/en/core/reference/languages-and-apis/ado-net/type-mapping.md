# ADO.NET Supported Types and Their Mappings

The following lists the built-in mappings for reading and writing CLR types to YDB types.

## Type Mapping Table for Reading

The following shows the mappings used when reading values.

These are the return types when using `YdbCommand.ExecuteScalarAsync()`, `YdbDataReader.GetValue()`, and similar methods.

| {{ ydb-short-name }} type     | .NET type                           |
|-------------------------------|-------------------------------------|
| `Bool`                        | `bool`                              |
| `Text` (synonym of `Utf8`)    | `string`                            |
| `Bytes` (synonym of `String`) | `byte[]`                            |
| `Uint8`                       | `byte`                              |
| `Uint16`                      | `ushort`                            |
| `Uint32`                      | `uint`                              |
| `Uint64`                      | `ulong`                             |
| `Int8`                        | `sbyte`                             |
| `Int16`                       | `short`                             |
| `Int32`                       | `int`                               |
| `Int64`                       | `long`                              |
| `Float`                       | `float`                             |
| `Double`                      | `double`                            |
| `Date`                        | `DateTime`                          |
| `Datetime`                    | `DateTime`                          |
| `Timestamp`                   | `DateTime`                          |
| `Decimal`                     | [see the Decimal section](#decimal) |
| `Json`                        | `string`                            |
| `JsonDocument`                | `string`                            |
| `Yson`                        | `byte[]`                            |

## Decimal {#decimal}

Decimal is a parameterized {{ ydb-short-name }} type (see the [documentation for details](../../../yql/reference/types/primitive.md#numeric)). `Precision` is the total number of significant digits; `Scale` is the number of digits after the decimal point.

By default, `Decimal(22, 9)` is used. To сhange it, set `Precision` and `Scale` attributes on the `YdbParameter` object.

Example: for `Decimal(5, 3)`, pass `new YdbParameter { Value = 1.5m, Precision = 5, Scale = 3 }`.

## Type Mapping Table for Writing

| {{ ydb-short-name }} type     | DbType                                                                                    | .NET type                    |
|-------------------------------|-------------------------------------------------------------------------------------------|------------------------------|
| `Bool`                        | `Boolean`                                                                                 | `bool`                       |
| `Text` (synonym of `Utf8`)    | `String`, `AnsiString`, `AnsiStringFixedLength`, `StringFixedLength`                      | `string`                     |
| `Bytes` (synonym of `String`) | `Binary`                                                                                  | `byte[]`                     |
| `Uint8`                       | `Byte`                                                                                    | `byte`                       |
| `Uint16`                      | `UInt16`                                                                                  | `ushort`                     |
| `Uint32`                      | `UInt32`                                                                                  | `uint`                       |
| `Uint64`                      | `UInt64`                                                                                  | `ulong`                      |
| `Int8`                        | `SByte`                                                                                   | `sbyte`                      |
| `Int16`                       | `Int16`                                                                                   | `short`                      |
| `Int32`                       | `Int32`                                                                                   | `int`                        |
| `Int64`                       | `Int64`                                                                                   | `long`                       |
| `Float`                       | `Single`                                                                                  | `float`                      |
| `Double`                      | `Double`                                                                                  | `double`                     |
| `Date`                        | `Date`                                                                                    | `DateTime`                   |
| `Datetime`                    | `DateTime`                                                                                | `DateTime`                   |
| `Timestamp`                   | `DateTime2` (for .NET type `DateTime`), `DateTimeOffset` (for .NET type `DateTimeOffset`) | `DateTime`, `DateTimeOffset` |
| `Decimal($p, $s)`             | `Decimal`, `Currency`                                                                     | `decimal`                    |

{% note info %}

It's important to understand that if the `DbType` is not specified, the parameter will be inferred from the `System.Type`.

{% endnote %}

You can also specify any {{ ydb-short-name }} type using the constructors from `Ydb.Sdk.Value.YdbValue`. For example:

```с#
var parameter = new YdbParameter("$parameter", YdbValue.MakeJsonDocument("{\"type\": \"jsondoc\"}")); 
```
