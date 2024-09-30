# Использование Dapper

Dapper - это микро ORM (Object Relational Mapping), который предоставляет простой и гибкий способ для взаимодействия с
базами данных. Он работает на основе стандарта ADO.NET и предлагает множество функций, которые облегчают работу с базой
данных.

## ADO.NET

ADO.NET — это набор классов, предоставляющих доступ к данным для разработчиков, использующих платформу .NET Framework.

YDB SDK C# поставляет набор классов реализующих стандарт ADO.NET.

### Установка 

Реализация ADO.NET для YDB доступен через [NuGet](https://www.nuget.org/packages/Ydb.Sdk/). 

### Создание подключения

Подключение к YDB устанавливается через YdbConnection.

1.  **Использование конструктора без параметров**:

    Следующий код создаёт подключение с настройками по умолчанию:
    
    ```c#
    await using var ydbConnection = new YdbConnection();
    await ydbConnection.OpenAsync();
    ```
    
    Этот вариант создаёт подключение к базе данных по URL: `grpc://localhost:2136/local`, с анонимной аутентификацией.

2. **Использование конструктора со строкой подключения**:

    В следующем примере происходит создание подключения при помощи строки подключения:
    
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

| Параметр          | Описание                                                                                           | Значение по умолчанию |
|-------------------|----------------------------------------------------------------------------------------------------|-----------------------|
| `Host`            | Указывает хост сервера                                                                             | `localhost`           |
| `Port`            | Определяет порт сервера                                                                            | `2136`                |
| `Database`        | Задаёт путь к базе данных, с которой будет происходить взаимодействие                              | `/local`              |
| `User`            | Значение задаёт имя пользователя                                                                   | Не определено         |
| `Password`        | Данный параметр задаёт пароль пользователя                                                         | Не определено         |
| `UseTls`          | Определяет, следует ли использовать протокол TLS (`grpc` или `grpcs`)                              | `false`               |
| `MaxSessionPool`  | Максимальное количество размер пула сессий                                                         | `100`                 |
| `RootCertificate` | Задаёт путь к доверенному сертификату сервера. Если этот параметр установлен, то UseTls будет true | Не определено         |

Существуют также и дополнительные параметры, которые не участвуют при формировании строки `ConnectionString`, их можно указать только используя `YdbConnectionStringBuilder`:

| Параметр               | Описание                                                       | Значение по умолчанию |
|------------------------|----------------------------------------------------------------|-----------------------|
| `LoggerFactory`        | Этот параметр служит фабрикой для создания классов логирования | Не определено         |
| `ICredentialsProvider` | Аутентифицирует пользователя, используя внешний IAM-провайдер  | Не определено         |

### Использование

Выполнение запросов осуществляется через объект `YdbCommand`:

```c#
await using var ydbConnection = new YdbConnection();
await ydbConnection.OpenAsync();

var ydbCommand = ydbConnection.CreateCommand();
ydbCommand.CommandText = "SELECT 'Hello world!'u";
Console.WriteLine(await ydbCommand.ExecuteScalarAsync());
```

Этот пример демонстрирует вывод в консоль `Hello world!`;

### Пользовательские транзакции

Для создания клиентской транзакции используйте метод `ydbConnection.BeginTransaction()`.

Опционально этот метод может принимать параметр типа `IsolationLevel`, который указывает уровень изоляции транзакции. Поддерживаются следующие уровни изоляции:

- `Serializable`: Обеспечивает полную изоляцию транзакции, используя оптимистические блокировки
- `Unspecified`: Позволяет базе данных определить самой подходящий уровень изоляции

Также вы можете указать параметр `TxMode`: {{ ydb-short-name }} специфичный уровень изоляции, ознакомиться поподробнее можно [здесь](../../concepts/transactions.md).

Уровень изоляции `Serializable`, используемый с параметром `TxMode.SerializableRW`, является эквивалентом вызова `BeginTransaction()` без параметров.

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

Параметры SQL-запроса можно задать с помощью класса `YdbParameter`:

```c#
await using var connection = new YdbConnection(_cmdOptions.SimpleConnectionString);
await connection.OpenAsync();

var ydbCommand = connection.CreateCommand();
ydbCommand.CommandText = """
                         DECLARE $series_id AS Uint64;
                         DECLARE $season_id AS Uint64;
                         DECLARE $limit_size AS Uint64;

                         SELECT series_id, season_id, episode_id, air_date, title
                         FROM episodes
                         WHERE series_id = $series_id AND season_id > $season_id
                         ORDER BY series_id, season_id, episode_id
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

В этом примере мы объявляем параметры `$series_id`, `$season_id` и `$limit_size` внутри SQL-запроса и затем добавляем их в команду с помощью `YdbParameter` объектов.

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
```

ADO.NET за вас подготовит запрос, чтобы переменные были стандарта YQL. А тип будет определен согласно `DbType` или `System.Type` самого значения.

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

### Свойства исключения YdbException

Исключение YdbException содержит следующие свойства, которые помогут вам более точно обработать ошибку:

- `IsTransient`: Возвращает true, если ошибка временная и может быть устранена повторной попыткой. Например, такая ошибка может возникнуть в случае нарушения блокировки транзакции, когда транзакция не успела завершить коммит.

- `IsTransientWhenIdempotent`: Возвращает true, если ошибка временная и может быть устранена в случае повтора операции, при условии, что операция с базой данных является идемпотентной.

- `StatusCode`: Содержит код ошибки базы данных, который может быть полезен для логирования и детального анализа проблемы.

## Dapper

Чтобы начать работу требуется дополнительная зависимость [Dapper](https://www.nuget.org/packages/Dapper/). 

