# Начало работы с ADO.NET

Лучший способ использовать `Ydb.Sdk` — установить его [NuGet-пакет](https://www.nuget.org/packages/Ydb.Sdk).

`Ydb.Sdk.Ado` стремится быть полностью совместимым с ADO.NET; его API должен быть практически идентичен другим драйверам баз данных .NET.

Вот базовый фрагмент кода, который поможет вам начать:

```c#
await using var connection = new YdbConnection("Host=localhost;Port=2136;Database=/local;MaxSessionPool=50");
await connection.OpenAsync();

var ydbCommand = connection.CreateCommand();
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

Вы можете найти более подробную информацию о ADO.NET API в документации [MSDN](https://learn.microsoft.com/ru-ru/dotnet/framework/data/adonet/ado-net-overview?redirectedfrom=MSDN) или во многих других руководствах в интернете.
