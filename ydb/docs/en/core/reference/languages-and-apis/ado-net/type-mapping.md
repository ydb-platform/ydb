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

`Decimal (Precision, Scale)` is a parameterized data type in {{ ydb-short-name }} that allows you to explicitly specify:

* `Precision` — the total number of significant digits;
* `Scale` — the number of digits after the decimal point.

For more details, see the [documentation](../../../yql/reference/types/primitive.md#numeric).

By default, the Decimal(22, 9) type is used. If you need to set different values for `Precision` and `Scale`, you can do this in your code.

The example below demonstrates how to store the value 1.5 in the database with the Decimal type and parameters Precision = 5 and Scale = 3.

```c#
await new YdbCommand(ydbConnection)
{
    CommandText = $"INSERT INTO {tableName}(Id, Decimal) VALUES (1, @Decimal);",
    Parameters = new YdbParameter { Name = "Decimal", Value = 1.5m, Precision = 5, Scale = 3 }
}.ExecuteNonQueryAsync();
```

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
