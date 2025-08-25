# Поддерживаемые типы данных в интеграции с ADO.NET и их соответствие

Ниже перечислены встроенные сопоставления при чтении и записи типов CLR в типы {{ ydb-short-name }}.

## Таблица сопоставления типов на чтение

Ниже показаны сопоставления, используемые при чтении значений.

Возвращаемый тип при использовании `YdbCommand.ExecuteScalarAsync()`, `YdbDataReader.GetValue()` и подобных методов.

| {{ ydb-short-name }} тип   | .NET тип   |
|----------------------------|------------|
| `Bool`                     | `bool`     |
| `Text` (синоним `Utf8`)    | `string`   |
| `Bytes` (синоним `String`) | `byte[]`   |
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

## Таблица сопоставления типов на запись

| {{ ydb-short-name }} тип   | DbType                                                                                    | .NET тип                     |
|----------------------------|-------------------------------------------------------------------------------------------|------------------------------|
| `Bool`                     | `Boolean`                                                                                 | `bool`                       |
| `Text` (синоним `Utf8`)    | `String`, `AnsiString`, `AnsiStringFixedLength`, `StringFixedLength`                      | `string`                     |
| `Bytes` (синоним `String`) | `Binary`                                                                                  | `byte[]`                     |
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
| `Timestamp`                | `DateTime2` (для .NET типа `DateTime`), `DateTimeOffset` (для .NET типа `DateTimeOffset`) | `DateTime`, `DateTimeOffset` |
| `Decimal(22,9)`            | `Decimal`, `Currency`                                                                     | `decimal`                    |

{% note info %}

Важно понимать, что если `DbType` не указан, параметр будет вычислен из `System.Type`.

{% endnote %}

Также вы можете указывать любой {{ ydb-short-name }} тип, используя конструкторы из `Ydb.Sdk.Value.YdbValue`. Например:

```с#
var parameter = new YdbParameter("$parameter", YdbValue.MakeJsonDocument("{\"type\": \"jsondoc\"}")); 
```
