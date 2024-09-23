# Приложение на C++

На этой странице подробно разбирается код [тестового приложения](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp/examples/basic_example), доступного в составе [C++ SDK](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp) {{ ydb-short-name }}.

{% include [init.md](steps/01_init.md) %}

Фрагмент кода приложения для инициализации драйвера:

```c++
    auto connectionParams = TConnectionsParams()
        .SetEndpoint(endpoint)
        .SetDatabase(database)
        .SetAuthToken(GetEnv("YDB_TOKEN"));

    TDriver driver(connectionParams);
```

Фрагмент кода приложения для создания клиента:

```c++
    TClient client(driver);
```

{% include [steps/02_create_table.md](steps/02_create_table.md) %}

Для создания таблиц используется метод `CreateTable`:

```c++
//! Creates sample tables with CrateTable API.
    ThrowOnError(client.RetryOperationSync([path](TSession session) {
        auto seriesDesc = TTableBuilder()
            .AddNonNullableColumn("series_id", EPrimitiveType::Uint64)
            .AddNullableColumn("title", EPrimitiveType::Utf8)
            .AddNullableColumn("series_info", EPrimitiveType::Utf8)
            .AddNullableColumn("release_date", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumn("series_id")
            .Build();

        return session.CreateTable(JoinPath(path, "series"), std::move(seriesDesc)).GetValueSync();
    }));
```

С помощью метода `DescribeTable` можно вывести информацию о структуре таблицы и убедиться, что она успешно создалась:

```c++
    TMaybe<TTableDescription> desc;

    ThrowOnError(client.RetryOperationSync([path, name, &desc](TSession session) {
        auto result = session.DescribeTable(JoinPath(path, name)).GetValueSync();

        if (result.IsSuccess()) {
            desc = result.GetTableDescription();
        }

        return result;
    }));

    Cout << "> Describe table: " << name << Endl;
    for (auto& column : desc->GetColumns()) {
        Cout << "Column, name: " << column.Name << ", type: " << FormatType(column.Type) << Endl;
    }
```

Приведенный фрагмент кода при запуске выводит на консоль текст:

```bash
> Describe table: series
Column, name: series_id, type: Uint64
Column, name: title, type: Utf8?
Column, name: series_info, type: Utf8?
Column, name: release_date, type: Uint64?
```

{% include [steps/03_write_queries.md](steps/03_write_queries.md) %}

Фрагмент кода, демонстрирующий выполнение запроса на запись/изменение данных:

```c++
//! Shows basic usage of mutating operations.
static TStatus UpsertSimpleTransaction(TSession session, const TString& path) {
    auto query = Sprintf(R"(
        PRAGMA TablePathPrefix("%s");

        UPSERT INTO episodes (series_id, season_id, episode_id, title) VALUES
            (2, 6, 1, "TBD");
    )", path.c_str());

    return session.ExecuteDataQuery(query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
}
```

{% include [pragmatablepathprefix.md](auxilary/pragmatablepathprefix.md) %}

{% include [steps/04_query_processing.md](steps/04_query_processing.md) %}

Для выполнения YQL-запросов используется метод `ExecuteDataQuery`.
SDK позволяет в явном виде контролировать выполнение транзакций и настраивать необходимый режим выполнения транзакций с помощью класса `TTxControl`.

В фрагменте кода, приведенном ниже, транзакция начинается методом `TTxControl::BeginTx`. С помощью `TTxSettings` устанавливается режим выполнения транзакции `SerializableRW`. После завершения всех запросов транзакции она будет автоматически завершена явным указанием: `CommitTx()`. Запрос `query`, описанный с помощью синтаксиса YQL, передается методу `ExecuteDataQuery` для выполнения.

```c++
//! Shows basic usage of YDB data queries and transactions.
static TStatus SelectSimpleTransaction(TSession session, const TString& path,
    TMaybe<TResultSet>& resultSet)
{
    auto query = Sprintf(R"(
        PRAGMA TablePathPrefix("%s");

        SELECT series_id, title, DateTime::ToDate(DateTime::FromDays(release_date)) AS release_date
        FROM series
        WHERE series_id = 1;
    )", path.c_str());

    auto txControl =
        // Begin new transaction with SerializableRW mode
        TTxControl::BeginTx(TTxSettings::SerializableRW())
        // Commit transaction at the end of the query
        .CommitTx();

    // Executes data query with specified transaction control settings.
    auto result = session.ExecuteDataQuery(query, txControl).GetValueSync();

    if (result.IsSuccess()) {
        // Index of result set corresponds to its order in YQL query
        resultSet = result.GetResultSet(0);
    }

    return result;
}
```

{% include [steps/05_results_processing.md](steps/05_results_processing.md) %}

Для обработки результатов выполнения запроса используется класс `TResultSetParser`.
Фрагмент кода, приведенный ниже, демонстрирует обработку результатов запроса с помощью объекта `parser`:

```c++
    TResultSetParser parser(*resultSet);
    if (parser.TryNextRow()) {
        Cout << "> SelectSimple:" << Endl << "Series"
            << ", Id: " << parser.ColumnParser("series_id").GetUint64()
            << ", Title: " << parser.ColumnParser("title").GetOptionalUtf8()
            << ", Release date: " << parser.ColumnParser("release_date").GetOptionalString()
            << Endl;
    }
```

Приведенный фрагмент кода при запуске выводит на консоль текст:

```bash
> SelectSimple:
series, Id: 1, title: IT Crowd, Release date: 2006-02-03
```



{% include [param_queries.md](steps/06_param_queries.md) %}

Фрагмент кода демонстрирует использование параметризованных запросов и `GetParamsBuilder` для формирования параметров и передачи их в `ExecuteDataQuery`:

```c++
//! Shows usage of parameters in data queries.
static TStatus SelectWithParamsTransaction(TSession session, const TString& path,
    ui64 seriesId, ui64 seasonId, TMaybe<TResultSet>& resultSet)
{
    auto query = Sprintf(R"(
        PRAGMA TablePathPrefix("%s");

        DECLARE $seriesId AS Uint64;
        DECLARE $seasonId AS Uint64;

        SELECT sa.title AS season_title, sr.title AS series_title
        FROM seasons AS sa
        INNER JOIN series AS sr
        ON sa.series_id = sr.series_id
        WHERE sa.series_id = $seriesId AND sa.season_id = $seasonId;
    )", path.c_str());

    // Type of parameter values should be exactly the same as in DECLARE statements.
    auto params = session.GetParamsBuilder()
        .AddParam("$seriesId")
            .Uint64(seriesId)
            .Build()
        .AddParam("$seasonId")
            .Uint64(seasonId)
            .Build()
        .Build();

    auto result = session.ExecuteDataQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        std::move(params)).GetValueSync();

    if (result.IsSuccess()) {
        resultSet = result.GetResultSet(0);
    }

    return result;
}
```

Приведенный фрагмент кода при запуске выводит на консоль текст:

```bash
> SelectWithParams:
Season, title: Season 3, series title: Silicon Valley
Finished preparing query: PreparedSelectTransaction
```

{% include [param_prep_queries.md](steps/07_param_prep_queries.md) %}

```c++
//! Shows usage of prepared queries.
static TStatus PreparedSelectTransaction(TSession session, const TString& path,
    ui64 seriesId, ui64 seasonId, ui64 episodeId, TMaybe<TResultSet>& resultSet)
{
    // Once prepared, query data is stored in the session and identified by QueryId.
    // Local query cache is used to keep track of queries, prepared in current session.
    auto query = Sprintf(R"(
        PRAGMA TablePathPrefix("%s");

        DECLARE $seriesId AS Uint64;
        DECLARE $seasonId AS Uint64;
        DECLARE $episodeId AS Uint64;

        SELECT *
        FROM episodes
        WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId;
    )", path.c_str());

    // Prepare query or get result from query cache
    auto prepareResult = session.PrepareDataQuery(query).GetValueSync();
    if (!prepareResult.IsSuccess()) {
        return prepareResult;
    }

    if (!prepareResult.IsFromCache()) {
        Cerr << "+Finished preparing query: PreparedSelectTransaction" << Endl;
    }

    auto dataQuery = prepareResult.GetQuery();

    auto params = dataQuery.GetParamsBuilder()
        .AddParam("$seriesId")
            .Uint64(seriesId)
            .Build()
        .AddParam("$seasonId")
            .Uint64(seasonId)
            .Build()
        .AddParam("$episodeId")
            .Uint64(episodeId)
            .Build()
        .Build();

    auto result = dataQuery.Execute(TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        std::move(params)).GetValueSync();

    if (result.IsSuccess()) {
        resultSet = result.GetResultSet(0);
    }

    return result;
}
```

Приведенный фрагмент кода при запуске выводит на консоль текст:

```bash
> PreparedSelect:
Episode 7, title: To Build a Better Beta, Air date: Sun Jun 05, 2016
```

Проверить наличие подготовленного запроса в сессии можно с помощью метода `GetPreparedQuery`. Если подготовленного запроса в контексте сессии еще не существует, его можно подготовить с помощью `PrepareDataQuery`, и сохранить для использования в рамках текущей сессии с помощью `AddPreparedQuery`.

{% include [multistep_transactions.md](steps/09_multistep_transactions.md) %}

Первый шаг — подготовка и выполнение первого запроса:

```c++
//! Shows usage of transactions consisting of multiple data queries with client logic between them.
static TStatus MultiStepTransaction(TSession session, const TString& path, ui64 seriesId, ui64 seasonId,
    TMaybe<TResultSet>& resultSet)
{
    auto query1 = Sprintf(R"(
        PRAGMA TablePathPrefix("%s");

        DECLARE $seriesId AS Uint64;
        DECLARE $seasonId AS Uint64;

        SELECT first_aired AS from_date FROM seasons
        WHERE series_id = $seriesId AND season_id = $seasonId;
    )", path.c_str());

    auto params1 = session.GetParamsBuilder()
        .AddParam("$seriesId")
            .Uint64(seriesId)
            .Build()
        .AddParam("$seasonId")
            .Uint64(seasonId)
            .Build()
        .Build();

    // Execute first query to get the required values to the client.
    // Transaction control settings don't set CommitTx flag to keep transaction active
    // after query execution.
    auto result = session.ExecuteDataQuery(
        query1,
        TTxControl::BeginTx(TTxSettings::SerializableRW()),
        std::move(params1)).GetValueSync();

    if (!result.IsSuccess()) {
        return result;
    }
```

Для продолжения работы в рамках текущей транзакции необходимо получить текущий `transaction id`:

```c++
    // Get active transaction id
    auto tx = result.GetTransaction();

    TResultSetParser parser(result.GetResultSet(0));
    parser.TryNextRow();
    auto date = parser.ColumnParser("from_date").GetOptionalUint64();

    // Perform some client logic on returned values
    auto userFunc = [] (const TInstant fromDate) {
        return fromDate + TDuration::Days(15);
    };

    TInstant fromDate = TInstant::Days(*date);
    TInstant toDate = userFunc(fromDate);
```

Следующий шаг — создание следующего запроса, использующего результаты выполнения кода на стороне клиентского приложения:

```c++
    // Construct next query based on the results of client logic
    auto query2 = Sprintf(R"(
        PRAGMA TablePathPrefix("%s");

        DECLARE $seriesId AS Uint64;
        DECLARE $fromDate AS Uint64;
        DECLARE $toDate AS Uint64;

        SELECT season_id, episode_id, title, air_date FROM episodes
        WHERE series_id = $seriesId AND air_date >= $fromDate AND air_date <= $toDate;
    )", path.c_str());

    auto params2 = session.GetParamsBuilder()
        .AddParam("$seriesId")
            .Uint64(seriesId)
            .Build()
        .AddParam("$fromDate")
            .Uint64(fromDate.Days())
            .Build()
        .AddParam("$toDate")
            .Uint64(toDate.Days())
            .Build()
        .Build();

    // Execute second query.
    // Transaction control settings continues active transaction (tx) and
    // commits it at the end of second query execution.
    result = session.ExecuteDataQuery(
        query2,
        TTxControl::Tx(*tx).CommitTx(),
        std::move(params2)).GetValueSync();

    if (result.IsSuccess()) {
        resultSet = result.GetResultSet(0);
    }

    return result;
}
```

Приведенные фрагменты кода при запуске выводит на консоль текст:

```bash
> MultiStep:
Episode 1, Season: 5, title: Grow Fast or Die Slow, Air date: Sun Mar 25, 2018
Episode 2, Season: 5, title: Reorientation, Air date: Sun Apr 01, 2018
Episode 3, Season: 5, title: Chief Operating Officer, Air date: Sun Apr 08, 2018
```

{% include [transaction_control.md](steps/10_transaction_control.md) %}

Фрагмент кода, демонстрирующий явное использование вызовов `BeginTransaction` и `tx.Commit()`:

```c++
// Show usage of explicit Begin/Commit transaction control calls.
// In most cases it's better to use transaction control settings in ExecuteDataQuery calls instead
// to avoid additional hops to YDB cluster and allow more efficient execution of queries.
static TStatus ExplicitTclTransaction(TSession session, const TString& path, const TInstant& airDate) {
    auto beginResult = session.BeginTransaction(TTxSettings::SerializableRW()).GetValueSync();
    if (!beginResult.IsSuccess()) {
        return beginResult;
    }

    // Get newly created transaction id
    auto tx = beginResult.GetTransaction();

    auto query = Sprintf(R"(
        PRAGMA TablePathPrefix("%s");

        DECLARE $airDate AS Date;

        UPDATE episodes SET air_date = DateTime::ToDays($airDate) WHERE title = "TBD";
    )", path.c_str());

    auto params = session.GetParamsBuilder()
        .AddParam("$airDate")
            .Date(airDate)
            .Build()
        .Build();

    // Execute data query.
    // Transaction control settings continues active transaction (tx)
    auto updateResult = session.ExecuteDataQuery(query,
        TTxControl::Tx(tx),
        std::move(params)).GetValueSync();

    if (!updateResult.IsSuccess()) {
        return updateResult;
    }

    // Commit active transaction (tx)
    return tx.Commit().GetValueSync();
}
```

