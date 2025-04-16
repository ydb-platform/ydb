# Использование Dapper

[Dapper](https://www.learndapper.com/) - это микро ORM (Object Relational Mapping), который предоставляет простой и гибкий способ для взаимодействия с базами данных. Он работает на основе стандарта ADO.NET и предлагает множество функций, которые облегчают работу с базой данных.

## ADO.NET

ADO.NET — это набор классов, предоставляющих доступ к данным для разработчиков, использующих платформу .NET Framework.

YDB SDK C# поставляет набор классов реализующих стандарт ADO.NET.

### Установка {#install}

Реализация ADO.NET для YDB доступен через [NuGet](https://www.nuget.org/packages/Ydb.Sdk/).

```dotnet
dotnet add package Ydb.Sdk
```

### Создание подключения

Подключение к YDB устанавливается через YdbConnection.

1. **Использование конструктора без параметров**:

   Следующий код создаёт подключение с настройками по умолчанию:

    ```c#
    await using var ydbConnection = new YdbConnection();
    await ydbConnection.OpenAsync();
    ```

   Этот вариант создаёт подключение к базе данных по URL: `grpc://localhost:2136/local`, с анонимной аутентификацией.

2. **Использование конструктора со строкой подключения**:

   В следующем примере происходит создание подключения при помощи [строки подключения в ADO.NET](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/connection-strings):

   ```C#
   await using var ydbConnection = new YdbConnection(
       "Host=database-sample-grpc;Port=2135;Database=/root/database-sample");
   await ydbConnection.OpenAsync();
   ```

   В данном случае подключение будет установлено по URL: `grpc://database-sample-grpc:2135/root/database-sample`. При использовании метода со строкой подключения параметры задаются в виде пар ключ=значение, разделённые точкой с запятой (`key1=value1;key2=value2`). Набор ключей имеет фиксированные значения, которые будут детально рассмотрены в следующих разделах.

3. **Использование конструктора с аргументом `YdbConnectionStringBuilder`**:

   Вариант с использованием `YdbConnectionStringBuilder` демонстрируется в коде ниже:

   ```c#
   var ydbConnectionBuilder = new YdbConnectionStringBuilder
   {
       Host = "server",
       Port = 2135,
       Database = "/ru-prestable/my-table",
       UseTls = true
   };
   await using var ydbConnection = new YdbConnection(ydbConnectionBuilder);
   await ydbConnection.OpenAsync();
   ```

### Параметры подключения

Все доступные параметры подключения определены как `property` в классе `YdbConnectionStringBuilder`.

Вот список параметров, которые можно задать в строке подключения `ConnectionString`:

| Параметр          | Описание                                                                                             | Значение по умолчанию |
|-------------------|------------------------------------------------------------------------------------------------------|-----------------------|
| `Host`            | Указывает хост сервера                                                                               | `localhost`           |
| `Port`            | Определяет порт сервера                                                                              | `2136`                |
| `Database`        | Задаёт путь к базе данных                                                                            | `/local`              |
| `User`            | Значение задаёт имя пользователя                                                                     | Не определено         |
| `Password`        | Данный параметр задаёт пароль пользователя                                                           | Не определено         |
| `UseTls`          | Определяет, следует ли использовать протокол TLS (`grpc` или `grpcs`)                                | `false`               |
| `MaxSessionPool`  | Максимальное количество размер пула сессий                                                           | `100`                 |
| `RootCertificate` | Задаёт путь к доверенному сертификату сервера. Если этот параметр установлен, то UseTls будет `true` | Не определено         |

Существуют также и дополнительные параметры, которые не участвуют при формировании строки `ConnectionString`, их можно указать только используя `YdbConnectionStringBuilder`:

| Параметр              | Описание                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | Значение по умолчанию        |
|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------|
| `LoggerFactory`       | Этот параметр принимает экземпляр, который реализует интерфейс [ILoggerFactory](https://learn.microsoft.com/en-us/dotnet/api/microsoft.extensions.logging.iloggerfactory). `ILoggerFactory` - это стандартный интерфейс для фабрик логирования в .NET. Возможно использование популярных фреймворков, таких как [NLog](https://github.com/NLog/NLog), [serilog](https://github.com/serilog/serilog), [log4net](https://github.com/apache/logging-log4net)                                                                                                                    | `NullLoggerFactory.Instance` |
| `CredentialsProvider` | Провайдер аутентификации, который реализует интерфейс `Ydb.Sdk.Auth.ICredentialsProvider`. [YDB SDK](#install) представляет несколько стандартных способов аутентификации: <br> 1) `Ydb.Sdk.Auth.AnonymousProvider`. Анонимный доступ к YDB, в основном для целей тестирования. <br> 2) `Ydb.Sdk.Auth.TokenProvider`. Аутентификация по токену, подобных OAuth. <br> 3) `Ydb.Sdk.Auth.StaticCredentialsProvider`. Аутентификация на основе имени пользователя и пароля. <br> Для Yandex.Cloud используете **[ydb-dotnet-yc](https://github.com/ydb-platform/ydb-dotnet-yc)** | `AnonymousProvider`          |

### Использование

Выполнение запросов осуществляется через объект `YdbCommand`:

```c#
await using var ydbConnection = new YdbConnection();
await ydbConnection.OpenAsync();

var ydbCommand = ydbConnection.CreateCommand();
ydbCommand.CommandText = "SELECT 'Hello world!'u";
Console.WriteLine(await ydbCommand.ExecuteScalarAsync());
```

Этот пример демонстрирует вывод в консоль `Hello world!`.

### Пользовательские транзакции

Для создания клиентской транзакции используйте метод `ydbConnection.BeginTransaction()`.

Есть две сигнатуры этого метода с единственным параметром уровня изоляции:

- `BeginTransaction(TxMode txMode)`<br>
   Параметр `Ydb.Sdk.Services.Query.TxMode` - это {{ ydb-short-name }} специфичный уровень изоляции, ознакомиться поподробнее можно [здесь](../../concepts/transactions.md).

- `BeginTransaction(IsolationLevel isolationLevel)`<br>
   Параметр `System.Data.IsolationLevel` из стандарта ADO.NET. Поддерживаются следующие уровни изоляции: `Serializable` и `Unspecified`. Оба эквивалентны параметру `TxMode.SerializableRW`.

Вызов `BeginTransaction()` без параметров открывает транзакцию с уровнем `TxMode.SerializableRW`.

Рассмотрим пример использования транзакции:

```c#
await using var ydbConnection = new YdbConnection();
await ydbConnection.OpenAsync();

var ydbCommand = ydbConnection.CreateCommand();

ydbCommand.Transaction = ydbConnection.BeginTransaction();
ydbCommand.CommandText = """
                            UPSERT INTO episodes (series_id, season_id, episode_id, title, air_date)
                            VALUES (2, 5, 13, "Test Episode", Date("2018-08-27"))
                         """;
await ydbCommand.ExecuteNonQueryAsync();

ydbCommand.CommandText = """
                         INSERT INTO episodes(series_id, season_id, episode_id, title, air_date)
                         VALUES
                             (2, 5, 21, "Test 21", Date("2018-08-27")),
                             (2, 5, 22, "Test 22", Date("2018-08-27"))
                         """;
await ydbCommand.ExecuteNonQueryAsync();
await ydbCommand.Transaction.CommitAsync();
```

Здесь открывается транзакция уровня `Serializable` и происходит две ставки в таблицу `episodes`.

### Использование параметров

Параметры YQL-запроса можно задать с помощью класса `YdbParameter`:

```c#
await using var connection = new YdbConnection(_cmdOptions.SimpleConnectionString);
await connection.OpenAsync();

var ydbCommand = connection.CreateCommand();
ydbCommand.CommandText = """
                         DECLARE $series_id AS Uint64;
                         DECLARE $season_id AS Uint64;
                         DECLARE $limit_size AS Uint64;
                         
                         SELECT series_id, season_id, episode_id, air_date, title
                         FROM episodes WHERE series_id = $series_id AND season_id > $season_id
                         ORDER BY $series_id, $season_id, $episode_id
                         LIMIT $limit_size;
                         """;
ydbCommand.Parameters.Add(new YdbParameter("$series_id", DbType.UInt64, 1U));
ydbCommand.Parameters.Add(new YdbParameter("$season_id", DbType.UInt64, 1U));
ydbCommand.Parameters.Add(new YdbParameter("$limit_size", DbType.UInt64, 3U));

var ydbDataReader = await ydbCommand.ExecuteReaderAsync();

_logger.LogInformation("Selected rows:");
while (await ydbDataReader.ReadAsync())
{
    _logger.LogInformation(
        "series_id: {series_id}, season_id: {season_id}, episode_id: {episode_id}, air_date: {air_date}, title: {title}",
        ydbDataReader.GetUint64(0), ydbDataReader.GetUint64(1), ydbDataReader.GetUint64(2),
        ydbDataReader.GetDateTime(3), ydbDataReader.GetString(4));
}
```

В этом примере мы объявляем параметры `series_id`, `season_id` и `limit_size` внутри YQL-запроса и затем добавляем их в команду с помощью `YdbParameter` объектов.

### Альтернативный стиль с префиксом @

Параметры также можно указывать с использованием префикса `@`. В этом случае не требуется предварительное объявление переменных в самом запросе. Запрос будет выглядеть следующим образом:

```c#
ydbCommand.CommandText = """
                         SELECT series_id, season_id, episode_id, air_date, title
                         FROM episodes
                         WHERE series_id = @series_id AND season_id > @season_id
                         ORDER BY series_id, season_id, episode_id
                         LIMIT @limit_size;
                         """;
ydbCommand.Parameters.Add(new YdbParameter("$series_id", DbType.UInt64, 1U));
ydbCommand.Parameters.Add(new YdbParameter("$season_id", DbType.UInt64, 1U));
ydbCommand.Parameters.Add(new YdbParameter("$limit_size", DbType.UInt64, 3U));                         
```

ADO.NET за вас подготовит запрос, чтобы переменные соответствовали [YQL](../../yql/reference/index.md). А тип будет определен согласно [DbType](https://learn.microsoft.com/en-us/dotnet/api/system.data.dbtype) или .NET тип самого значения.

### Таблица сопоставления типов на чтение

Ниже показаны сопоставления, используемые при чтении значений.

Возвращаемый тип при использовании `YdbCommand.ExecuteScalarAsync()`, `YdbDataReader.GetValue()` и подобные методы.

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

### Таблица сопоставления типов на запись

| {{ ydb-short-name }}       | DbType                                                                                    | .Net types                   |
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

Важно понимать, что если `DbType` не указан, параметр будет вычислен из `System.Type`.

Также вы можете указывать любой {{ ydb-short-name }} тип, используя конструкторы из `Ydb.Sdk.Value.YdbValue`. Например:

```с#
var parameter = new YdbParameter("$parameter", YdbValue.MakeJsonDocument("{\"type\": \"jsondoc\"}")); 
```

### Обработка ошибок

Все исключения, связанные с операциями в базе данных, являются подклассами `YdbException`.

Для безопасной обработки ошибок, которые могут возникнуть при выполнении команд, вы можете использовать блок `try-catch`. Пример:

```c#
try
{
    await command.ExecuteNonQueryAsync();
}
catch (YdbException e)
{
    Console.WriteLine($"Error executing command: {e}");
}
```

### Свойства исключения `YdbException`

Исключение `YdbException` содержит следующие свойства, которые помогут вам более точно обработать ошибку:

- `IsTransient`: Возвращает true, если ошибка временная и может быть устранена повторной попыткой. Например, такая ошибка может возникнуть в случае нарушения блокировки транзакции, когда транзакция не успела завершить коммит.

- `IsTransientWhenIdempotent`: Возвращает true, если ошибка временная и может быть устранена в случае повтора операции, при условии, что операция с базой данных является идемпотентной.

- `StatusCode`: Содержит код ошибки базы данных, который может быть полезен для логирования и детального анализа проблемы.

{% note warning %}

Обратите внимание, что ADO.NET не выполняет автоматических попыток повторения, и вам нужно реализовать повтор попытки в своем коде.

{% endnote %}

## Интеграция YDB и Dapper

Чтобы начать работу требуется дополнительная зависимость [Dapper](https://www.nuget.org/packages/Dapper/).

Рассмотрим полный пример:

```c#
using Dapper;
using Ydb.Sdk.Ado;

await using var connection = await new YdbDataSource().OpenConnectionAsync();

await connection.ExecuteAsync("""
                              CREATE TABLE Users(
                                  Id Int32,
                                  Name Text,
                                  Email Text,
                                  PRIMARY KEY (Id)   
                              );
                              """);

await connection.ExecuteAsync("INSERT INTO Users(Id, Name, Email) VALUES (@Id, @Name, @Email)",
    new User { Id = 1, Name = "Name", Email = "Email" });

Console.WriteLine(await connection.QuerySingleAsync<User>("SELECT * FROM Users WHERE Id = @Id", new { Id = 1 }));

await connection.ExecuteAsync("DROP TABLE Users");

internal class User
{
    public int Id { get; init; }
    public string Name { get; init; } = null!;
    public string Email { get; init; } = null!;

    public override string ToString()
    {
        return $"Id: {Id}, Name: {Name}, Email: {Email}";
    }
}
```

За дополнительной информацией обратитесь к официальной [документации](https://www.learndapper.com/).

### Важные аспекты

Для того чтобы Dapper интерпретировал `DateTime` как {{ ydb-short-name }} тип `Datetime`. Выполните следующий код:

```c#
SqlMapper.AddTypeMap(typeof(DateTime), DbType.DateTime);
```

По умолчанию, `DateTime` интерпретируется как `Timestamp`.
