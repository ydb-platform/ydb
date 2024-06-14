# App in C# (.NET)

This page contains a detailed description of the code of a [test app](https://github.com/ydb-platform/ydb-dotnet-examples) that uses the {{ ydb-short-name }} [C# (.NET) SDK](https://github.com/ydb-platform/ydb-dotnet-sdk).

{% include [addition.md](auxilary/addition.md) %}

{% include [steps/01_init.md](steps/01_init.md) %}

App code snippet for driver initialization:

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

App code snippet for creating a session:

```c#
using var tableClient = new TableClient(driver, new TableClientConfig());
```

{% include [steps/02_create_table.md](steps/02_create_table.md) %}

To create tables, use the `session.ExecuteSchemeQuery` method with a DDL (Data Definition Language) YQL query.

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

Code snippet for data insert/update:

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

To execute YQL queries, use the `Session.executeDataQuery()` method. The SDK lets you explicitly control the execution of transactions and configure the transaction execution mode using the `TxControl` class. In the code snippet below, a transaction with the `SerializableRW` mode and an automatic commit after executing the request is used. The values of the request parameters are passed in the form of a dictionary name-value in the `parameters` argument.

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

The result of query execution (resultset) consists of an organized set of rows. Example of processing the query execution result:

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
