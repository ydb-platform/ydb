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
using var queryClient = new QueryService(driver);
```

{% include [steps/02_create_table.md](steps/02_create_table.md) %}

To create tables, use the `queryClient.Exec` method with a DDL (Data Definition Language) YQL query.

```c#
    await queryClient.Exec(@"
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
```

{% include [steps/03_write_queries.md](steps/03_write_queries.md) %}

Code snippet for data insert/update:

```c#
await _queryClient.Exec(@"
    DECLARE $id AS Uint64;
    DECLARE $title AS Utf8;
    DECLARE $release_date AS Date;

    UPSERT INTO series (series_id, title, release_date) VALUES
        ($id, $title, $release_date);
    ",
    new Dictionary<string, YdbValue>
    {
        { "$id", YdbValue.MakeUint64(1) },
        { "$title", YdbValue.MakeUtf8("NewTitle") },
        { "$release_date", YdbValue.MakeDate(DateTime.UtcNow) }
    }
);
```

{% include [pragmatablepathprefix.md](auxilary/pragmatablepathprefix.md) %}

{% include [steps/04_query_processing.md](steps/04_query_processing.md) %}

To execute YQL queries, use the `queryClient.ReadRow` или `queryClient.ReadAllRows` method. The SDK lets you explicitly control the execution of transactions and configure the transaction execution mode using the `TxMode` enum. In the code snippet below, a transaction with the `NoTx` mode and an automatic commit after executing the request is used. The values of the request parameters are passed in the form of a dictionary name-value in the `parameters` argument.

```c#
var row = await _queryClient.ReadRow(@"
        DECLARE $id AS Uint64;

        SELECT
            series_id,
            title,
            release_date
        FROM series
        WHERE series_id = $id;
    ",
    new Dictionary<string, YdbValue>
    {
        { "$id", YdbValue.MakeUint64(id) }
    }
);
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
await queryClient.Stream(
    $"SELECT title FROM seasons ORDER BY series_id, season_id LIMIT {sizeSeasons} OFFSET 9",
    async stream =>
    {
        await foreach (var part in stream)
        {
            foreach (var row in part.ResultSet!.Rows)
            {
                Console.WriteLine(row[0].GetOptionalUtf8());
            }
        }
    });
```
