# ADO.NET Supported Types and Their Mappings

The following lists the built-in mappings for reading and writing CLR types to YDB types.

## Type Mapping Table for Reading

The following shows the mappings used when reading values.

- These are the return types when using `YdbCommand.ExecuteScalarAsync()`, `YdbDataReader.GetValue()`, and similar methods.
- You can read as other types by calling `YdbDataReader.GetFieldValue<T>()` and also `YdbDataReader.GetInt32()` / `YdbDataReader.GetInt64()` to get the raw value.

| {{ ydb-short-name }} тип   | .NET тип                                 | Non-default .NET типы                     |
|----------------------------|------------------------------------------|-------------------------------------------|
| Bool                       | bool                                     |                                           |
| Int8                       | sbyte                                    |                                           |
| Int16                      | short                                    |                                           |
| Int32                      | int                                      |                                           |
| Int64                      | long                                     |                                           |
| Uint8                      | byte                                     |                                           |
| Uint16                     | ushort                                   |                                           |
| Uint32                     | uint                                     |                                           |
| Uint64                     | ulong                                    |                                           |
| Float                      | float                                    |                                           |
| Double                     | double                                   |                                           |
| Decimal (precision, scale) | decimal ([cм. раздел Decimal](#decimal)) |                                           |
| Bytes (синоним `String`)   | byte[]                                   |                                           |
| Text (синоним `Utf8`)      | string                                   |                                           |
| Json                       | string                                   |                                           |
| JsonDocument               | string                                   |                                           |
| Yson                       | byte[]                                   |                                           |
| Uuid                       | Guid                                     |                                           |
| Date                       | DateTime                                 | DateOnly                                  |
| Date32                     | DateTime                                 | DateOnly, int (`GetInt32()` — raw value)  |
| Datetime                   | DateTime                                 | DateOnly                                  |
| Datetime64                 | DateTime                                 | DateOnly, long (`GetInt64()` — raw value) |
| Timestamp                  | DateTime                                 | DateOnly                                  |
| Timestamp64                | DateTime                                 | DateOnly, long (`GetInt64()` — raw value) |
| Interval                   | TimeSpan                                 | long (`GetInt64()` — raw value)           |
| Interval64                 | TimeSpan                                 | long (`GetInt64()` — raw value)           |

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

There are three rules that determine the YDB type sent for a parameter:

1. If the parameter's `YdbDbType` is set, it is used.
2. If the parameter's `DbType` is set, it is used.
3. If none of the above is set, the backend type will be inferred from the CLR value type.

| {{ ydb-short-name }} type   | .NET type     | Non-default .NET types                | YdbDbType         | DbType                                                       |
|-----------------------------|---------------|---------------------------------------|-------------------|--------------------------------------------------------------|
| Bool                        | bool          |                                       | Bool              | Boolean                                                      |
| Int8                        | sbyte         |                                       | Int8              | SByte                                                        |
| Int16                       | short         | sbyte, byte                           | Int16             | Int16                                                        |
| Int32                       | int           | sbyte, byte, short, ushort            | Int32             | Int32                                                        |
| Int64                       | long          | sbyte, byte, short, ushort, int, uint | Int64             | Int64                                                        |
| Uint8                       | byte          |                                       | Uint8             | Byte                                                         |
| Uint16                      | ushort        | byte                                  | Uint16            | UInt16                                                       |
| Uint32                      | uint          | byte, ushort                          | Uint32            | UInt32                                                       |
| Uint64                      | ulong         | byte, ushort, uint                    | Uint64            | UInt64                                                       |
| Float                       | float         |                                       | Float             | Single                                                       |
| Double                      | double        | float                                 | Double            | Double                                                       |
| Decimal (precision, scale)  | decimal       |                                       | Decimal           | Decimal, Currency                                            |
| Bytes (synonym of `String`) | byte[]        |                                       | Bytes             | Binary                                                       |
| Text (synonym of `Utf8`)    | string        |                                       | Text              | String, AnsiString, AnsiStringFixedLength, StringFixedLength |
| Json                        | string        |                                       | Json              |                                                              |
| JsonDocument                | string        |                                       | JsonDocument      |                                                              |
| Yson                        | byte[]        |                                       | Yson              |                                                              |
| Uuid                        | Guid          |                                       | Uuid              | Guid                                                         |
| Date                        | DateOnly      | DateTime                              | Date              | Date                                                         |
| Date32                      |               | DateTime, DateOnly, int (raw value)   | Date32            |                                                              |
| Datetime                    |               | DateTime, DateOnly                    | Datetime          | Datetime                                                     |
| Datetime64                  |               | DateTime, long (raw value)            | Datetime64        |                                                              |
| Timestamp                   | DateTime      |                                       | Timestamp         | DateTime2                                                    |
| Timestamp64                 |               | DateTime, long (raw value)            | Timestamp64       |                                                              |
| Interval                    | TimeSpan      | long (raw value)                      | Interval          |                                                              |
| Interval64                  |               | TimeSpan, long (raw value)            | Interval64        |                                                              |
| List                        | T[], List<T>  | T[], List<T>                          | List \| YdbDbType |                                                              |

{% note info %}

When using List, specify the type as a bitwise OR of YdbDbType.List and the element type. For example, to specify the YdbDbType for List<Int32>, use `YdbDbType.List | YdbDbType.Int32`.

{% endnote %}