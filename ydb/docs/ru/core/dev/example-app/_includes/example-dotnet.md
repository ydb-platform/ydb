# Приложение на C#

На этой странице подробно разбирается код [тестового приложения](https://github.com/ydb-platform/ydb-dotnet-sdk/examples), использующего [C# SDK](https://github.com/ydb-platform/ydb-dotnet-sdk) {{ ydb-short-name }}.

{% include [steps/01_init.md](steps/01_init.md) %}

Фрагмент кода приложения для подключения к базе данных:

```c#
using Ydb.Sdk.Ado;

await using var dataSource = new YdbDataSource("Host=localhost;Port=2136;Database=/local");
await using var connection = await dataSource.OpenConnectionAsync();
```

{% include [steps/02_create_table.md](steps/02_create_table.md) %}

Для создания таблиц используется `YdbCommand` с DDL (Data Definition Language) YQL-запросом:

```c#
await using var command = new YdbCommand(connection)
{
    CommandText = @"
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
        );"
};
await command.ExecuteNonQueryAsync();
```

{% include [steps/03_write_queries.md](steps/03_write_queries.md) %}

Фрагмент кода, демонстрирующий выполнение запроса на запись/изменение данных:

```c#
await using var command = new YdbCommand(@"
    UPSERT INTO series (series_id, title, release_date) VALUES
        ($id, $title, $release_date);
    ", connection);
command.Parameters.Add(new YdbParameter("$id", YdbDbType.Uint64, 1UL));
command.Parameters.Add(new YdbParameter("$title", YdbDbType.Text, "NewTitle"));
command.Parameters.Add(new YdbParameter("$release_date", YdbDbType.Date, DateTime.UtcNow));
await command.ExecuteNonQueryAsync();
```

{% include [steps/04_query_processing.md](steps/04_query_processing.md) %}

Для выполнения YQL-запросов с чтением данных используется метод `ExecuteReaderAsync`. Параметры запроса передаются через коллекцию `Parameters` объекта `YdbCommand`:

```c#
await using var command = new YdbCommand(@"
    SELECT
        series_id,
        title,
        release_date
    FROM series
    WHERE series_id = $id;
    ", connection);
command.Parameters.Add(new YdbParameter("$id", YdbDbType.Uint64, id));
await using var reader = await command.ExecuteReaderAsync();
```

{% include [steps/05_results_processing.md](steps/05_results_processing.md) %}

Результат выполнения запроса обрабатывается через `DbDataReader`. Пример обработки результата:

```c#
while (await reader.ReadAsync())
{
    Console.WriteLine($"> Series, " +
        $"series_id: {reader.GetUint64(0)}, " +
        $"title: {reader.GetString(1)}, " +
        $"release_date: {reader.GetDateTime(2)}");
}
```

<<<<<<< HEAD
{% include [scan_query.md](steps/08_scan_query.md) %}
=======
Для последовательного чтения строк из другого запроса:
>>>>>>> 317adb799 (dev: update dotnet snippets (#38018))

```c#
await using var command = new YdbCommand(
    "SELECT title FROM seasons ORDER BY series_id, season_id;", connection);
await using var reader = await command.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    Console.WriteLine(reader.GetString(0));
}
```