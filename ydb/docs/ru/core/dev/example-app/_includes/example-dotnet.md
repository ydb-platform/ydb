# Приложение на C# (.NET)

На этой странице подробно разбирается код [тестового приложения](https://github.com/ydb-platform/ydb-dotnet-examples), использующего [C# (.NET) SDK](https://github.com/ydb-platform/ydb-dotnet-sdk) {{ ydb-short-name }}.

{% include [addition.md](auxilary/addition.md) %}

{% include [steps/01_init.md](steps/01_init.md) %}

Фрагмент кода приложения для инициализации драйвера:

```c#
public static async Task Run(
    string endpoint,
    string database,
    ICredentialsProvider credentialsProvider)
{
    var config = new DriverConfig(
        endpoint: endpoint,
        database: database,
        credentials: credentialsProvider
    );

    using var driver = new Driver(
        config: config
    );

    await driver.Initialize();
}
```

Фрагмент кода приложения для создания сессии:

```c#
using var tableClient = new TableClient(driver, new TableClientConfig());
```

{% include [steps/02_create_table.md](steps/02_create_table.md) %}

Для создания таблиц используется метод `session.ExecuteSchemeQuery` с DDL (Data Definition Language) YQL-запросом.

```c#
var response = await tableClient.SessionExec(async session =>
{
    return await session.ExecuteSchemeQuery(@"
        CREATE TABLE series (
            series_id Uint64 NOT NULL,
            title Utf8,
            series_info Utf8,
            release_date Date,
            PRIMARY KEY (series_id)
        );

        CREATE TABLE seasons (
            series_id Uint64,
            season_id Uint64,
            title Utf8,
            first_aired Date,
            last_aired Date,
            PRIMARY KEY (series_id, season_id)
        );

        CREATE TABLE episodes (
            series_id Uint64,
            season_id Uint64,
            episode_id Uint64,
            title Utf8,
            air_date Date,
            PRIMARY KEY (series_id, season_id, episode_id)
        );
    ");
});

response.Status.EnsureSuccess();
```

{% include [steps/03_write_queries.md](steps/03_write_queries.md) %}

Фрагмент кода, демонстрирующий выполнение запроса на запись/изменение данных:

```c#
var response = await tableClient.SessionExec(async session =>
{
    var query = @"
        DECLARE $id AS Uint64;
        DECLARE $title AS Utf8;
        DECLARE $release_date AS Date;

        UPSERT INTO series (series_id, title, release_date) VALUES
            ($id, $title, $release_date);
    ";

    return await session.ExecuteDataQuery(
        query: query,
        txControl: TxControl.BeginSerializableRW().Commit(),
        parameters: new Dictionary<string, YdbValue>
            {
                { "$id", YdbValue.MakeUint64(1) },
                { "$title", YdbValue.MakeUtf8("NewTitle") },
                { "$release_date", YdbValue.MakeDate(DateTime.UtcNow) }
            }
    );
});

response.Status.EnsureSuccess();
```

{% include [pragmatablepathprefix.md](auxilary/pragmatablepathprefix.md) %}

{% include [steps/04_query_processing.md](steps/04_query_processing.md) %}

Для выполнения YQL-запросов используется метод `session.ExecuteDataQuery()`. SDK позволяет в явном виде контролировать выполнение транзакций и настраивать необходимый режим выполнения транзакций с помощью класса `TxControl`. В фрагменте кода, приведенном ниже, используется транзакция с режимом `SerializableRW` и автоматическим коммитом после выполнения запроса. Значения параметров запроса передаются в виде словаря имя-значение в аргументе `parameters`.

```c#
var response = await tableClient.SessionExec(async session =>
{
    var query = @"
        DECLARE $id AS Uint64;

        SELECT
            series_id,
            title,
            release_date
        FROM series
        WHERE series_id = $id;
    ";

    return await session.ExecuteDataQuery(
        query: query,
        txControl: TxControl.BeginSerializableRW().Commit(),
        parameters: new Dictionary<string, YdbValue>
            {
                { "$id", YdbValue.MakeUint64(id) }
            },
    );
});

response.Status.EnsureSuccess();
var queryResponse = (ExecuteDataQueryResponse)response;
var resultSet = queryResponse.Result.ResultSets[0];
```

{% include [steps/05_results_processing.md](steps/05_results_processing.md) %}

Результат выполнения запроса (ResultSet) состоит из упорядоченного набора строк (Rows). Пример обработки результата выполнения запроса:

```c#
foreach (var row in resultSet.Rows)
{
    Console.WriteLine($"> Series, " +
        $"series_id: {(ulong)row["series_id"]}, " +
        $"title: {(string?)row["title"]}, " +
        $"release_date: {(DateTime?)row["release_date"]}");
}
```



{% include [scan_query.md](steps/08_scan_query.md) %}

```c#
public void executeScanQuery()
{
  var scanStream = TableClient.ExecuteScanQuery(@$"
    SELECT series_id, season_id, COUNT(*) AS episodes_count
    FROM episodes
    GROUP BY series_id, season_id
    ORDER BY series_id, season_id;
  ");

  while (await scanStream.Next())
  {
    scanStream.Response.EnsureSuccess();

    var resultSet = scanStream.Response.Result.ResultSetPart;
    if (resultSet != null)
    {
      foreach (var row in resultSet.Rows)
      {
        Console.WriteLine($"> ScanQuery, " +
          $"series_id: {(ulong)row["series_id"]}, " +
          $"season_id: {(ulong?)row["season_id"]}, " +
          $"episodes_count: {(ulong)row["episodes_count"]}");
      }
    }
  }
}
```
