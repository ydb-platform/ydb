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
using var queryClient = new QueryService(driver);
```

{% include [steps/02_create_table.md](steps/02_create_table.md) %}

Для создания таблиц используется метод `queryClient.Exec` с DDL (Data Definition Language) YQL-запросом.

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

Фрагмент кода, демонстрирующий выполнение запроса на запись/изменение данных:

```c#
await queryClient.Exec(@"
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

{% include [steps/04_query_processing.md](steps/04_query_processing.md) %}

Для чтения YQL-запросов используется методы `queryClient.ReadRow` или `queryClient.ReadAllRows`. SDK позволяет в явном виде контролировать выполнение транзакций и настраивать необходимый режим выполнения транзакций с помощью класса `TxMode`. Во фрагменте кода, приведенном ниже, используется транзакция с режимом `NoTx` и автоматическим коммитом после выполнения запроса. Значения параметров запроса передаются в виде словаря имя-значение в аргументе `parameters`.

```c#
var row = await queryClient.ReadRow(@"
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
await queryClient.Stream(
    $"SELECT title FROM seasons ORDER BY series_id, season_id;",
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
