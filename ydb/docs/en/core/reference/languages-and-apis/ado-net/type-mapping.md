# Supported Types and their Mappings

The following lists the built-in mappings when reading and writing CLR types to YDB types.

### Type mapping table for reading

The following shows the mappings used when reading values.

The return type when using  `YdbCommand.ExecuteScalarAsync()`, `YdbDataReader.GetValue()` and similar methods.

| {{ ydb-short-name }} type  | .NET type  |
|----------------------------|------------|
| `Bool`                     | `bool`     |
| `Text` (synonym `Utf8`)    | `string`   |
| `Bytes` (synonym `String`) | `byte[]`   |
| `Uint8`                    | `byte`     |
| `Uint16`                   | `ushort`   |
| `Uint32`                   | `uint`     |
| `Uint64`                   | `ulong`    |
| `Int8`                     | `sbyte`    |
| `Int16`                    | `short`    |
| `Int32`                    | `int`      |
| `Int64`                    | `long`     |
| `Float`                    | `float`    |
| `Double`                   | `double`   |
| `Date`                     | `DateTime` |
| `Datetime`                 | `DateTime` |
| `Timestamp`                | `DateTime` |
| `Decimal(22,9)`            | `Decimal`  |
| `Json`                     | `string`   |
| `JsonDocument`             | `string`   |
| `Yson`                     | `byte[]`   |

### Type mapping table for writing

| {{ ydb-short-name }} type  | DbType                                                                                    | .NET type                    |
|----------------------------|-------------------------------------------------------------------------------------------|------------------------------|
| `Bool`                     | `Boolean`                                                                                 | `bool`                       |
| `Text` (synonym `Utf8`)    | `String`, `AnsiString`, `AnsiStringFixedLength`, `StringFixedLength`                      | `string`                     |
| `Bytes` (synonym `String`) | `Binary`                                                                                  | `byte[]`                     |
| `Uint8`                    | `Byte`                                                                                    | `byte`                       |
| `Uint16`                   | `UInt16`                                                                                  | `ushort`                     |
| `Uint32`                   | `UInt32`                                                                                  | `uint`                       |
| `Uint64`                   | `UInt64`                                                                                  | `ulong`                      |
| `Int8`                     | `SByte`                                                                                   | `sbyte`                      |
| `Int16`                    | `Int16`                                                                                   | `short`                      |
| `Int32`                    | `Int32`                                                                                   | `int`                        |
| `Int64`                    | `Int64`                                                                                   | `long`                       |
| `Float`                    | `Single`                                                                                  | `float`                      |
| `Double`                   | `Double`                                                                                  | `double`                     |
| `Date`                     | `Date`                                                                                    | `DateTime`                   |
| `Datetime`                 | `DateTime`                                                                                | `DateTime`                   |
| `Timestamp`                | `DateTime2` (for .NET type `DateTime`), `DateTimeOffset` (for .NET type `DateTimeOffset`) | `DateTime`, `DateTimeOffset` |
| `Decimal(22,9)`            | `Decimal`, `Currency`                                                                     | `decimal`                    |

It's important to understand that if the `DbType` is not specified, the parameter will be inferred from the `System.Type`.

You can also specify any {{ ydb-short-name }} type using the constructors from `Ydb.Sdk.Value.YdbValue`. For example:

```с#
var parameter = new YdbParameter("$parameter", YdbValue.MakeJsonDocument("{\"type\": \"jsondoc\"}")); 
```
