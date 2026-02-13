# Поддерживаемые типы данных в интеграции с ADO.NET и их соответствие

Ниже перечислены встроенные сопоставления при чтении и записи типов CLR в типы {{ ydb-short-name }}.

## Таблица сопоставления типов на чтение

Ниже показаны сопоставления, используемые при чтении значений.

- Возвращаемый тип при использовании `YdbCommand.ExecuteScalarAsync()`, `YdbDataReader.GetValue()` и подобных методов.
- Вы можете читать значения также как другие .NET‑типы, вызывая `YdbDataReader.GetFieldValue<T>()`. Для некоторых временных типов можно получить их «сырое» (непреобразованное) значение, как оно хранится в базе — через `GetInt32()` или `GetInt64()`.

| **{{ ydb-short-name }} тип**       | **.NET тип**                                       | **Non-default .NET типы**                                 |
|------------------------------------|----------------------------------------------------|-----------------------------------------------------------|
| `Bool`                            | `bool`                                             |                                                           |
| `Int8`                            | `sbyte`                                            |                                                           |
| `Int16`                           | `short`                                            |                                                           |
| `Int32`                           | `int`                                              |                                                           |
| `Int64`                           | `long`                                             |                                                           |
| `Uint8`                           | `byte`                                             |                                                           |
| `Uint16`                          | `ushort`                                           |                                                           |
| `Uint32`                          | `uint`                                             |                                                           |
| `Uint64`                          | `ulong`                                            |                                                           |
| `Float`                           | `float`                                            |                                                           |
| `Double`                          | `double`                                           |                                                           |
| `Decimal (precision, scale)`      | `decimal` ([см. раздел Decimal](#decimal))        |                                                           |
| `Bytes` (синоним ``String``)      | `byte[]`                                           |                                                           |
| `Text` (синоним ``Utf8``)         | `string`                                           |                                                           |
| `Json`                            | `string`                                           |                                                           |
| `JsonDocument`                    | `string`                                           |                                                           |
| `Yson`                            | `byte[]`                                           |                                                           |
| `Uuid`                            | `Guid`                                             |                                                           |
| `Date`                            | `DateTime`                                         | `DateOnly`                                                |
| `Date32`                          | `DateTime`                                         | `DateOnly`, `int` (`GetInt32()` — «сырой» value)        |
| `Datetime`                        | `DateTime`                                         | `DateOnly`                                                |
| `Datetime64`                      | `DateTime`                                         | `DateOnly`, `long` (`GetInt64()` — «сырой» value)       |
| `Timestamp`                       | `DateTime`                                         | `DateOnly`                                                |
| `Timestamp64`                     | `DateTime`                                         | `DateOnly`, `long` (`GetInt64()` — «сырой» value)       |
| `Interval`                        | `TimeSpan`                                         | `long`(`GetInt64()` — «сырой» value)                   |
| `Interval64`                      | `TimeSpan`                                         | `long`(`GetInt64()` — «сырой» value)                   |

## Decimal {#decimal}

`Decimal (Precision, Scale)` — это параметрический тип данных в YDB, который позволяет явно указать:

* `Precision` — общее количество значащих цифр;
* `Scale` — количество цифр после запятой.

Подробнее — в [документации](../../../yql/reference/types/primitive.md#numeric)

По умолчанию используется тип Decimal(22, 9). Если нужно задать другие значения Precision и Scale, это можно сделать с помощью кода.

Пример ниже показывает, как записать в базу данных значение 1.5 с типом Decimal и параметрами Precision = 5 и Scale = 3.

```c#
await new YdbCommand(ydbConnection)
{
    CommandText = $"INSERT INTO {tableName}(Id, Decimal) VALUES (1, @Decimal);",
    Parameters = new YdbParameter { Name = "Decimal", Value = 1.5m, Precision = 5, Scale = 3 }
}.ExecuteNonQueryAsync();
```

## Таблица сопоставления типов на запись

Существует три правила, определяющие тип параметра в {{ ydb-short-name }}:

1. Если для параметра YdbDbType задано значение, оно используется.
2. Если для параметра задан DbType, он используется.
3. Если ничего из вышеперечисленного не задано, тип серверной части будет определен на основе типа значения ([CLR](https://ru.wikipedia.org/wiki/Common_Language_Runtime)).

| **{{ ydb-short-name }} тип** | **.NET тип** | **Non-default .NET типы**               | **YdbDbType**     | **DbType**                                                   |
|------------------------------|--------------|-----------------------------------------|-------------------|--------------------------------------------------------------|
| `Bool`                      | `bool`       |                                         | `Bool`            | `Boolean`                                                    |
| `Int8`                      | `sbyte`      |                                         | `Int8`            | `SByte`                                                      |
| `Int16`                     | `short`      | `sbyte`, `byte`                         | `Int16`           | `Int16`                                                      |
| `Int32`                     | `int`        | `sbyte`, `byte`, `short`, `ushort`      | `Int32`           | `Int32`                                                      |
| `Int64`                     | `long`       | `sbyte`, `byte`, `short`, `ushort`, `int`, `uint` | `Int64`           | `Int64`                                                      |
| `Uint8`                     | `byte`       |                                         | `Uint8`           | `Byte`                                                       |
| `Uint16`                    | `ushort`     | `byte`                                  | `Uint16`          | `UInt16`                                                     |
| `Uint32`                    | `uint`       | `byte`, `ushort`                        | `Uint32`          | `UInt32`                                                     |
| `Uint64`                    | `ulong`      | `byte`, `ushort`, `uint`                | `Uint64`          | `UInt64`                                                     |
| `Float`                     | `float`      |                                         | `Float`           | `Single`                                                     |
| `Double`                    | `double`     | `float`                                 | `Double`          | `Double`                                                     |
| `Decimal (precision, scale)`| `decimal`    |                                         | `Decimal`         | `Decimal`, `Currency`                                        |
| `Bytes (синоним String)`    | `byte[]`     |                                         | `Bytes`           | `Binary`                                                     |
| `Text (синоним Utf8)`       | `string`     |                                         | `Text`            | `String`, `AnsiString`, `AnsiStringFixedLength`, `StringFixedLength` |
| `Json`                      | `string`     |                                         | `Json`            |                                                              |
| `JsonDocument`              | `string`     |                                         | `JsonDocument`    |                                                              |
| `Yson`                      | `byte[]`     |                                         | `Yson`            |                                                              |
| `Uuid`                      | `Guid`       |                                         | `Uuid`            | `Guid`                                                       |
| `Date`                      | `DateOnly`   | `DateTime`                              | `Date`            | `Date`                                                       |
| `Date32`                    |              | `DateTime`, `DateOnly`, `int` («сырой» value) | `Date32`      |                                                              |
| `Datetime`                  |              | `DateTime`, `DateOnly`                  | `Datetime`        | `Datetime`                                                   |
| `Datetime64`                |              | `DateTime`, `long` («сырой» value)      | `Datetime64`      |                                                              |
| `Timestamp`                 | `DateTime`   |                                         | `Timestamp`       | `DateTime2`                                                  |
| `Timestamp64`               |              | `DateTime`, `long` («сырой» value)      | `Timestamp64`     |                                                              |
| `Interval`                  | `TimeSpan`   | `long` («сырой» value)                  | `Interval`        |                                                              |
| `Interval64`                |              | `TimeSpan`, `long` («сырой» value)      | `Interval64`      |                                                              |
| `List`                      | `T[]`, `List<T>` | `T[]`, `List<T>`                    | `List` \| `YdbDbType` |                                                      |

{% note info %}

При использовании List указывайте тип как побитовое **OR** YdbDbType.List с типом элемента. Например, чтобы задать YdbDbType для List<Int32>, используйте `YdbDbType.List | YdbDbType.Int32`.

{% endnote %}