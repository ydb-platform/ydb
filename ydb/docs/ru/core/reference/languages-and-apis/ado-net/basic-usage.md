# Основное использование ADO.NET

В этой статье рассматриваются основные сценарии использования ADO.NET с YDB, включая подключение к базе данных, выполнение запросов и обработку результатов. Дополнительные сведения см. в основной [документации](index.md).

## Data Source {#data_source}

Отправной точкой для любой операции с базой данных является [DbDataSource](https://learn.microsoft.com/en-us/dotnet/api/system.data.common.dbdatasource).

YdbDataSource можно создать несколькими способами.

### Без параметров

Следующий код создаёт источник данных с настройками по умолчанию:

```c#
await using var ydbDataSource = new YdbDataSource();
```

В этом случае используется адрес подключения `grpc://localhost:2136/local` и анонимная аутентификация.

### С использованием строки подключения

Пример создания с помощью [строки подключения ADO.NET](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/connection-strings):

```c#
await using var ydbDataSource = new YdbDataSource(
    "Host=database-sample-grpc;Port=2135;Database=/root/database-sample");
```

Источник данных будет использовать URL: `grpc://database-sample-grpc:2135/root/database-sample`. Поддерживаемый набор настроек описан на [странице параметров подключения](connection-parameters.md).

### Использование конструктора с аргументом `YdbConnectionStringBuilder`

```c#
var ydbConnectionBuilder = new YdbConnectionStringBuilder
{
    Host = "localhost",
    Port = 2136,
    Database = "/local",
    UseTls = false
};

await using var ydbDataSource = new YdbDataSource(ydbConnectionBuilder);
```

`YdbConnectionStringBuilder` поддерживает дополнительные [настройки](connection-parameters.md#connection-builder-parameters), помимо строки подключения, такие как логирование, расширенные параметры аутентификации.

## Подключения

Подключение к {{ ydb-short-name }} устанавливается через `YdbConnection`. Получение подключений осуществляется через `YdbDataSource`, это можно сделать через следующие методы:

### YdbDataSource.OpenConnectionAsync

Открывает подключение к YDB с параметрами, указанными при создании `YdbDataSource` (см. [раздел Ydb Data Source](#data_source)).

```c#
await using var ydbConnection = await ydbDataSource.OpenConnectionAsync();
```

Рекомендуется для длинных читающих запросов.

### YdbDataSource.OpenRetryableConnectionAsync

Открывает подключение с автоматическими повторами операций (retry), учитывающее YDB Retry Policy (см. [раздел Повторные попытки](#retry_policy)).

```c#
await using var ydbConnection = ydbDataSource.OpenRetryableConnectionAsync();
```

Особенности режима:

- Транзакции не поддерживаются. Попытка использовать транзакцию приведет к исключению (см. [раздел Транзакции](#transactions)).
- Команды (`YdbCommand`), созданные из такого подключения, автоматически повторяют одиночные операции при временных ошибках.

{% note warning %}

При чтении будьте аккуратны с «длинными» выборками: они могут привести к переполнению памяти (Out Of Memory, OOM), поскольку результирующий набор считывается целиком для получения финальных статусов от сервера.

{% endnote %}

### YdbDataSource.CreateConnection

{% note warning %}

Использование `YdbDataSource.CreateConnection` и конструктора `YdbConnection` (legacy API ADO.NET) не рекомендуется. По возможности используйте современные способы открытия подключения через `YdbDataSource.OpenConnectionAsync` или `YdbDataSource.OpenRetryableConnectionAsync`.

{% endnote %}

Если необходим именно этот способ — откройте подключение вручную:

- Вариант со строкой подключения:

```c#
await using var ydbConnection = new YdbConnection(
    "Host=database-sample-grpc;Port=2135;Database=/root/database-sample");
await ydbConnection.OpenAsync(); 
```

- Вариант с `YdbConnectionStringBuilder`:

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

- Вариант `YdbDataSource.CreateConnection`:

```c#
await using var ydbConnection = ydbDataSource.CreateConnection();
await ydbConnection.OpenAsync();
```

## Pooling

Открытие нового подключения к {{ ydb-short-name }} — затратная операция, поэтому провайдер использует пул подключений. При закрытии или освобождении объекта подключения оно не закрывается — экземпляр возвращается в пул, управляемый `Ydb.Sdk.Ado`. При следующем запросе подключение будет повторно использовано из пула. Это делает операции открытия и закрытия быстрыми: открывайте и закрывайте подключения по мере необходимости, не удерживайте их открытыми без нужды.

{% note info %}

Пул работает для подключений, открытых через `YdbDataSource` (например, `OpenConnectionAsync`/`OpenRetryableConnectionAsync`), а также для подключений, созданных вручную через конструктор `YdbConnection` с последующим вызовом `OpenAsync()`.

{% endnote %}

{% note info %}

Как это устроено внутри: для прикладного кода «подключение» — логическое. Под капотом операции — это RPC‑вызовы поверх небольшого пула gRPC/HTTP/2‑каналов. Провайдер также управляет пулом сессий таблиц. Эти детали прозрачны для пользователя и настраиваются параметрами пула (см. [параметры Pooling](connection-parameters.md#pooling)).

{% endnote %}

Очистить пул сессий и закрыть все физические подключения с узлами YDB:

- `YdbDataSource.DisposeAsync()`: Освобождает источник данных. Закрывает связанные с ним пулы и сетевые каналы для его `ConnectionString`.

    ```c#
   await YdbDataSource.DisposeAsync()
   ```

- `YdbConnection.ClearPool`: Немедленно закрывает все незанятые подключения в пуле, связанном с `ConnectionString` указанного подключения. Активные подключения будут закрыты при возврате в пул.

   ```c#
   await YdbConnection.ClearPool(ydbConnection)
   ```

- `YdbConnection.ClearAllPools()`: Закрывает все незанятые соединения во всех пулах. Активные соединения закрываются при возврате.

    ```c#
    await YdbConnection.ClearAllPools()
    ```

## Базовое выполнение SQL

Как только у вас есть `YdbConnection`, для выполнения SQL-запроса можно использовать `YdbCommand`:

```c#
await using var ydbCommand = new YdbCommand("SELECT some_field FROM some_table", ydbConnection);
await using var ydbDataReader = await ydbCommand.ExecuteReaderAsync();

while (await ydbDataReader.ReadAsync())
{
    Console.WriteLine(ydbDataReader.GetString(0));
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

Например, чтобы выполнить простой SQL `INSERT`, который ничего не возвращает, вы можете использовать `ExecuteNonQueryAsync` следующим образом:

```c#
await using var ydbCommand = new YdbCommand("INSERT INTO some_table (some_field) VALUES ('Hello YDB!'u)", ydbConnection);
await ydbCommand.ExecuteNonQueryAsync();
```

## Параметры

При отправке пользовательских данных в базу данных всегда рекомендуется использовать параметры, а не включать значения в SQL следующим образом:

```c#
await using var ydbConnection = await ydbDataSource.OpenConnectionAsync();

var ydbCommand = ydbConnection.CreateCommand();
ydbCommand.CommandText = """
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

ADO.NET за вас подготовит запрос, чтобы переменные соответствовали [YQL](../../../yql/reference/index.md). Тип каждого параметра будет определен по YdbDbType, при его отсутствии — по [DbType](https://learn.microsoft.com/en-us/dotnet/api/system.data.dbtype), иначе выводится из .NET‑типа значения.

## Parameter types

{{ ydb-short-name }} имеет строго типизированную систему типов: столбцы и параметры имеют тип, а типы обычно не преобразуются неявным образом в другие типы. Это означает, что вам нужно подумать о том, какой тип вы будете отправлять: попытка вставить строку в целочисленный столбец (или наоборот) приведет к сбою.

Для получения дополнительной информации о поддерживаемых типах и их сопоставления смотрите эту [страницу](type-mapping.md).

## Транзакции {#transactions}

Интерактивные транзакции и повторы (retry) — ключевая часть работы с {{ ydb-short-name }}.

YdbDataSource предоставляет вспомогательные методы, упрощающие выполнение кода в транзакции с автоматическими повторами.

```c#
await ydbDataSource.ExecuteInTransactionAsync(async ydbConnection =>
{
    var count = (int)(await new YdbCommand(ydbConnection)
        { CommandText = $"SELECT count FROM {tableName} WHERE id = 1" }
        .ExecuteScalarAsync())!;

    await new YdbCommand(ydbConnection)
    {
        CommandText = $"UPDATE {tableName} SET count = @count + 1 WHERE id = 1",
        Parameters = { new YdbParameter { Value = count, ParameterName = "count" } }
    }.ExecuteNonQueryAsync();
},
new YdbRetryPolicyConfig { MaxAttempts = 5 });
```

Повторные попытки выполняются в соответствии с политиками YDB (см. [раздел Повторные попытки](#retry_policy)).

### Транзакции YdbConnection

Создать транзакцию можно стандартным способом ADO.NET:

```c#
await using var ydbConnection = await ydbDataSource.OpenConnectionAsync();
await using var ydbTransaction = await connection.BeginTransactionAsync();
// ... команды в транзакции ...
await transaction.CommitAsync();
```

{% note warning %}

В таком случае обработка ошибок ([Transaction Lock Invalidated](https://ydb.tech/docs/ru/troubleshooting/performance/queries/transaction-lock-invalidation)) становится заботой пользователя. YDB может откатить транзакцию в случае инвалидации MVC локов.

{% endnote %}

{% note info %}

Это допустимый и рекомендуемый для длительных чтений. Используйте транзакции чтения в режиме [snapshot read-only](../../../concepts/transactions.md#modes). Такие транзакции дают консистентный срез, не берут блокировки записи и минимизируют конфликты.

{% endnote %}

Есть две сигнатуры этого метода с единственным параметром уровня изоляции:

- `BeginTransaction(TransactionMode transactionMode)`<br>
  Параметр `Ydb.Sdk.Ado.TransactionMode` — это {{ ydb-short-name }}-специфичный уровень изоляции, подробнее ознакомиться можно [здесь](../../../concepts/transactions.md).

- `BeginTransaction(IsolationLevel isolationLevel)`<br>
  Параметр `System.Data.IsolationLevel` из стандарта ADO.NET. Поддерживаются следующие уровни изоляции: `Serializable` и `Unspecified`. Оба эквивалентны параметру `TxMode.SerializableRW`.

Вызов `BeginTransaction()` без параметров открывает транзакцию с уровнем `TxMode.SerializableRW`.

{{ ydb-short-name }} не поддерживает вложенные или параллельные транзакции. В любой момент времени может выполняться только одна транзакция, и при запуске новой транзакции, в то время как другая уже выполняется, возникает исключение. Следовательно, нет необходимости передавать объект `YdbTransaction`, возвращаемый функцией `BeginTransaction()` в команды, которые вы выполняете. При запуске транзакции все последующие команды автоматически включаются до тех пор, пока не будет выполнена фиксация или откат. Однако для обеспечения максимальной переносимости лучше всего явно задать область транзакции для ваших команд.

## Повторные попытки {#retry_policy}

Повторы (retry) — важная часть дизайна {{ ydb-short-name }}. Провайдер ADO.NET предоставляет гибкую политику повторов с учетом специфики {{ ydb-short-name }}.

Рекомендации по выбору подхода:

- Одиночные пишущие операции (без интерактивных транзакций): используйте `YdbDataSource.OpenRetryableConnectionAsync`.
- Транзакционные сценарии: используйте семейство методов `YdbDataSource.ExecuteInTransactionAsync`.
- Выполнение кода с автоматическими повторами вне транзакции: используйте семейство `YdbDataSource.ExecuteAsync`
- Долгие читающие операции: используйте обычный `YdbConnection`. Для консистентных «снимков» данных выполняйте чтение в read-only Snapshot‑транзакции. Не используйте retry‑подключение для «длинных» чтений, чтобы избежать избыточного буферизации результатов.

### Передача настроек политики

В методы `OpenRetryableConnectionAsync`, `ExecuteInTransactionAsync`, `ExecuteAsync` можно передать `YdbRetryPolicyConfig`:

- Подключение с повторами:

    ```c#
    await using var conn = await ydbDataSource.OpenRetryableConnectionAsync(
        new YdbRetryPolicyConfig { MaxAttempts = 5 });
    ```

- Транзакция с повторами:

    ```c#
    await ydbDataSource.ExecuteInTransactionAsync(
        async conn => { /* ваш код */ },
        new YdbRetryPolicyConfig { MaxAttempts = 5 });
    ```

- Выполнение блока кода с повторами:

    ```c#
    await ydbDataSource.ExecuteAsync(
        async conn => { /* ваш код */ },
        new YdbRetryPolicyConfig { MaxAttempts = 5 });
    ```

| **Параметр**           | **Описание**                                                                                                                                                                                   | **Значение по умолчанию** |
|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------|
| `MaxAttempts`            | Общее число попыток, включая первую. Значение 1 полностью отключает повторы.                                                                                                                   | `10`                        |
| `EnableRetryIdempotence` | Включает повторы для статусов с неизвестным результатом выполнения на сервере. Используйте только для идемпотентных операций — иначе возможен повторный эффект выполнения.                     | `false`                     |
| `FastBackoffBaseMs`      | Базовая задержка (мс) для быстрых повторов: ошибки, которые обычно быстро проходят (например, временная недоступность, TLI — Transaction Lock Invalidated). Экспоненциальный backoff с jitter. | `5`                         |
| `FastCapBackoffMs`       | Максимальная задержка (мс) для быстрых повторов. Экспоненциальный backoff с jitter не превышает этот предел.                                                                                   | `500`                       |
| `SlowBackoffBaseMs`      | Базовая задержка (мс) для «медленных» повторов: перегрузка, исчерпание ресурсов и т.п. Экспоненциальный backoff с jitter.                                                                      | `50`                        |
| `SlowCapBackoffMs`       | Максимальная задержка (мс) для «медленных» повторов. Экспоненциальный backoff с jitter не превышает этот предел.                                                                               | `5000`                      |

### Кастомная политика повторных попыток

Для крайних случаев можно реализовать собственную политику, реализовав интерфейс `Ydb.Sdk.Ado.Retry.IRetryPolicy`. Политика получает `YdbException` и номер текущей попытки (attempt) и должна вернуть решение — повторять ли операцию и с какой задержкой.

{% note warning %}

Используя данный подход, вы точно должны быть уверены в том, что хотите, так как вы отказываетесь от рекомендуемых настроек.

{% endnote %}

## Обработка ошибок

{% note info %}

Этот раздел актуален, если вы не используете встроенные повторы провайдера (см. [раздел Повторные попытки](#retry_policy)).

{% endnote %}

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

Примеры приведены на GitHub по [ссылке](https://github.com/ydb-platform/ydb-dotnet-sdk/tree/main/examples).
