# Основное использование ADO.NET

В этой статье рассматриваются основные сценарии использования ADO.NET c YDB, включая подключение к базе данных, выполнение запросов и обработку результатов. Дополнительные сведения см. в основной [документации](index.md).

## Подключения

Подключение к {{ ydb-short-name }} устанавливается через `YdbConnection`.

1. **Использование пустой строки подключения**:

   Следующий код создаёт подключение с настройками по умолчанию:

    ```c#
    await using var ydbConnection = new YdbConnection("");
    await ydbConnection.OpenAsync();
    ```

   Этот вариант создаёт подключение к базе данных по URL: `grpc://localhost:2136/local`, с анонимной аутентификацией.

2. **Использование конструктора со строкой подключения**:

   В следующем примере происходит создание подключения при помощи [строки подключения в ADO.NET](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/connection-strings):

   ```c#
   await using var ydbConnection = new YdbConnection(
       "Host=database-sample-grpc;Port=2135;Database=/root/database-sample");
   await ydbConnection.OpenAsync();
   ```

   В данном случае подключение будет установлено по URL: `grpc://database-sample-grpc:2135/root/database-sample`. Поддерживаемый набор настроек описан на [странице параметров подключения](connection-parameters.md).

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

   `YdbConnectionStringBuilder` поддерживает дополнительные [настройки](connection-parameters.md#connection-builder-parameters), помимо строки подключения, такие как логирование, расширенные параметры аутентификации.

## Pooling

Открытие и закрытие логического соединения с {{ ydb-short-name }} является дорогостоящим и трудоемким процессом. Поэтому соединения с {{ ydb-short-name }} объединяются в пул. Закрытие или удаление соединения не проводит к закрытию основного логического соединения; скорее, она возвращается в пул, управляемый `Ydb.Sdk.Ado`. Когда соединение снова понадобится, будет возвращено соединение из пула. Это делает операции открытия и закрытия быстрыми. Не стесняйтесь часто открывать и закрывать соединения, если это необходимо, вместо того, чтобы без необходимости поддерживать соединение открытым в течении длительного периода времени.

### ClearPool

Немедленно закрывает незанятые соединения. Активные соединения закрываются при возврате.

```c#
YdbConnection.ClearPool(ydbConnection)
```

### ClearAllPools

Закрывает все незанятые соединения во всех пулах. Активные соединения закрываются при возврате.

```c#
YdbConnection.ClearAllPools()
```

## Data Source

Начиная с .NET 7.0, отправной точкой для любой операции с базой данных является [DbDataSource](https://learn.microsoft.com/en-us/dotnet/api/system.data.common.dbdatasource).

Самый простой способ создать источник данных заключается в следующем:

```c#
await using var dataSource = new YdbDataSource("Host=localhost;Port=2136;Database=/local");
```

Или

```c#
var ydbConnectionBuilder = new YdbConnectionStringBuilder
{
    Host = "localhost",
    Port = 2136,
    Database = "/local",
    UseTls = false
};

await using var dataSource = new YdbDataSource(ydbConnectionBuilder);
```

## Базовое выполнение SQL

Как только у вас есть `YdbConnection`, для выполнения SQL-запроса можно использовать `YdbCommand`:

```c#
await using var command = dataSource.CreateCommand("SELECT some_field FROM some_table")
await using var reader = await command.ExecuteReaderAsync();

while (await reader.ReadAsync())
{
    Console.WriteLine(reader.GetString(0));
}
```

## Другие методы выполнения

Выше мы выполнили SQL с помощью [ExecuteReaderAsync](https://learn.microsoft.com/ru-ru/dotnet/api/system.data.common.dbcommand.executereaderasync). Существуют различные способы выполнения команды, в зависимости от того, каких результатов вы от нее ожидаете:

1. [ExecuteNonQueryAsync](https://learn.microsoft.com/ru-ru/dotnet/api/system.data.common.dbcommand.executenonqueryasync): выполняет SQL, который не возвращает никаких результатов, обычно это инструкции `INSERT`, `UPDATE` или `DELETE`.

   {% note warning %}

   {{ ydb-short-name }} не возвращает количество затронутых строк.

   {% endnote %}

2. [ExecuteScalarAsync](https://learn.microsoft.com/ru-ru/dotnet/api/system.data.common.dbcommand.executescalarasync): выполняет SQL, который возвращает единственное скалярное значение.
3. [ExecuteReaderAsync](https://learn.microsoft.com/ru-ru/dotnet/api/system.data.common.dbcommand.executereaderasync): выполняет SQL, который вернет полный результирующий набор. Возвращает `YdbDataReader`, который можно использовать для доступа к результирующему набору (как в приведенном выше примере).

Например, чтобы выполнить простой SQL `INSERT`, который ничего не возвращает, вы можете использовать ExecuteNonQueryAsync следующим образом:

```c#
await using var command = dataSource.CreateCommand("INSERT INTO some_table (some_field) VALUES ('Hello YDB!'u)");
await command.ExecuteNonQueryAsync();
```

## Параметры

При отправке пользовательских данных в базу данных всегда рекомендуется использовать параметры, а не включать значения в SQL следующим образом:

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
                         ORDER BY series_id, season_id, episode_id
                         LIMIT $limit_size;
                         """;
ydbCommand.Parameters.Add(new YdbParameter("$series_id", DbType.UInt64, 1U));
ydbCommand.Parameters.Add(new YdbParameter("$season_id", DbType.UInt64, 1U));
ydbCommand.Parameters.Add(new YdbParameter("$limit_size", DbType.UInt64, 3U));

var ydbDataReader = await ydbCommand.ExecuteReaderAsync();
```

Параметры SQL-запроса можно задать с помощью класса `YdbParameter`.

В этом примере параметры `$series_id`, `$season_id` и `$limit_size` объявляются в SQL-запросе и затем добавляются в команду с помощью объектов `YdbParameter`.

## Альтернативный стиль параметра с префиксом `@`

Параметры также можно указывать с использованием префикса `@`. В этом случае не требуется предварительное объявление переменных в самом запросе. Запрос будет выглядеть следующим образом:

```c#
ydbCommand.CommandText = """
                         SELECT series_id, season_id, episode_id, air_date, title
                         FROM episodes
                         WHERE series_id = @series_id AND season_id > @season_id
                         ORDER BY series_id, season_id, episode_id
                         LIMIT @limit_size;
                         """;
ydbCommand.Parameters.Add(new YdbParameter("series_id", DbType.UInt64, 1U));
ydbCommand.Parameters.Add(new YdbParameter("season_id", DbType.UInt64, 1U));
ydbCommand.Parameters.Add(new YdbParameter("limit_size", DbType.UInt64, 3U));
```

ADO.NET за вас подготовит запрос, чтобы переменные соответствовали [YQL](../../../yql/reference/index.md). А тип будет определен согласно [DbType](https://learn.microsoft.com/en-us/dotnet/api/system.data.dbtype) или .NET тип самого значения.

## Parameter types

{{ ydb-short-name }} имеет строго типизированную систему типов: столбцы и параметры имеют тип, а типы обычно не преобразуются неявным образом в другие типы. Это означает, что вам нужно подумать о том, какой тип вы будете отправлять: попытка вставить строку в целочисленный столбец (или наоборот) приведет к сбою.

Для получения дополнительной информации о поддерживаемых типах и их сопоставления смотрите эту [страницу](type-mapping.md).

## Транзакции

Чтобы создать клиентскую транзакцию, используйте стандартный метод ADO.NET `ydbConnection.BeginTransaction()`.

Есть две сигнатуры этого метода с единственным параметром уровня изоляции:

- `BeginTransaction(TxMode txMode)`<br>
  Параметр `Ydb.Sdk.Services.Query.TxMode` - это {{ ydb-short-name }} специфичный уровень изоляции, ознакомиться поподробнее можно [здесь](../../../concepts/transactions.md).

- `BeginTransaction(IsolationLevel isolationLevel)`<br>
  Параметр `System.Data.IsolationLevel` из стандарта ADO.NET. Поддерживаются следующие уровни изоляции: `Serializable` и `Unspecified`. Оба эквивалентны параметру `TxMode.SerializableRW`.

Вызов `BeginTransaction()` без параметров открывает транзакцию с уровнем `TxMode.SerializableRW`.

Рассмотрим пример использования транзакции:

```c#
await using var connection = await dataSource.OpenConnectionAsync();
await using var transaction = await connection.BeginTransactionAsync();

await using var command1 = new YdbCommand(connection) { CommandText = "...", Transaction = transaction };
await command1.ExecuteNonQueryAsync();

await using var command2 = new YdbCommand(connection) { CommandText = "...", Transaction = transaction };
await command2.ExecuteNonQueryAsync();

await transaction.CommitAsync();
```

{{ ydb-short-name }} не поддерживает вложенные или параллельные транзакции. В любой момент времени может выполняться только одна транзакция, и при запуске новой транзакции, в то время как другая уже выполняется, возникает исключение. Следовательно, нет необходимости передавать объект `YdbTransaction`, возвращаемый функцией `BeginTransaction()` в команды, которые вы выполняете. При запуске транзакции все последующие команды автоматически включаются до тех пор, пока не будет выполнена фиксация или откат. Однако для обеспечения максимальной переносимости лучше всего явно задать область транзакции для ваших команд.

## Обработка ошибок

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

## Примеры

Примеры приведены на GitHub по [ссылке](https://github.com/ydb-platform/ydb-dotnet-sdk/tree/main/examples/src/AdoNet).
