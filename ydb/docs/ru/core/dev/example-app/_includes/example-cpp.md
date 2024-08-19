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

```c++
//! Creates sample tables with ExecuteQuery Query Service
    ThrowOnError(client.RetryQuery([path](TSession session) {
        auto query = Sprintf(R"(
            PRAGMA TablePathPrefix("%s");
            CREATE TABLE series (
                series_id Uint64,
                title Utf8,
                series_info Utf8,
                release_date Uint64,
                PRIMARY KEY (series_id)
            );
        )", path.c_str());
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }));
```

{% include [steps/03_write_queries.md](steps/03_write_queries.md) %}

Фрагмент кода, демонстрирующий выполнение запроса на запись/изменение данных:

```c++
//! Shows basic usage of mutating operations.
static TAsyncExecuteQueryResult UpsertSimpleTransaction(TSession session, const TString& path) {
    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        UPSERT INTO episodes (series_id, season_id, episode_id, title) VALUES
            (2, 6, 1, "TBD");
    )", path.c_str());

    return session.ExecuteQuery(query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx());
}
```

{% include [pragmatablepathprefix.md](auxilary/pragmatablepathprefix.md) %}

{% include [steps/04_query_processing.md](steps/04_query_processing.md) %}


Для выполнения YQL-запросов используется метод `ExecuteQuery`.
SDK позволяет в явном виде контролировать выполнение транзакций и настраивать необходимый режим выполнения транзакций с помощью класса `TTxControl`.  

В фрагменте кода, приведенном ниже, транзакция начинается методом `TTxControl::BeginTx`. С помощью `TTxSettings` устанавливается режим выполнения транзакции `SerializableRW`. После завершения всех запросов транзакции она будет автоматически завершена явным указанием: `CommitTx()`. Запрос `query`, описанный с помощью синтаксиса YQL, передается методу `ExecuteQuery` для выполнения.

```c++
static TAsyncExecuteQueryResult SelectSimpleTransaction(TSession session, const TString& path)
{
    auto query = Sprintf(R"(
        PRAGMA TablePathPrefix("%s");

        SELECT series_id, title, CAST(CAST(release_date AS Date) AS String) AS release_date
        FROM series
        WHERE series_id = 1;
    )", path.c_str());

    auto txControl =
        // Begin new transaction with SerializableRW mode
        TTxControl::BeginTx(TTxSettings::SerializableRW())
        // Commit transaction at the end of the query
        .CommitTx();

    return session.ExecuteQuery(query, txControl);
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

Фрагмент кода демонстрирует использование параметризованных запросов и `TParamsBuilder` для формирования параметров и передачи их в `ExecuteQuery`:

```c++
static TAsyncExecuteQueryResult SelectWithParamsTransaction(TSession session, const TString& path,
    ui64 seriesId, ui64 seasonId)
{
    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        DECLARE $seriesId AS Uint64;
        DECLARE $seasonId AS Uint64;

        SELECT sa.title AS season_title, sr.title AS series_title
        FROM seasons AS sa
        INNER JOIN series AS sr
        ON sa.series_id = sr.series_id
        WHERE sa.series_id = $seriesId AND sa.season_id = $seasonId;
    )", path.c_str());

    auto params = TParamsBuilder()
        .AddParam("$seriesId")
            .Uint64(seriesId)
            .Build()
        .AddParam("$seasonId")
            .Uint64(seasonId)
            .Build()
        .Build();

    auto result = session.ExecuteQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        params);

    return result;
}
```

Приведенный фрагмент кода при запуске выводит на консоль текст:

```bash
> SelectWithParams:
Season, title: Season 3, series title: Silicon Valley
Finished preparing query: PreparedSelectTransaction
```

## Потоковые запросы {#stream-query}

Выполняется потоковый запрос данных, результатом исполнения которого является стрим. Стрим позволяет считать неограниченное количество строк и объем данных.

**ВНИМАНИЕ**: Не стоит использовать без RertyQuery.

```c++
// WARNING: Do not use without RetryQuery!!!
static TStatus StreamQuerySelectTransaction(TQueryClient client, const TString& path, std::vector<TResultSet>& resultSets) {
    resultSets.clear();
    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        DECLARE $series AS List<UInt64>;

        SELECT series_id, season_id, title, CAST(CAST(first_aired AS Date) AS String) AS first_aired
        FROM seasons
        WHERE series_id IN $series
        ORDER BY season_id;
    )", path.c_str());

    auto parameters = TParamsBuilder()
        .AddParam("$series")
        .BeginList()
            .AddListItem().Uint64(1)
            .AddListItem().Uint64(10)
        .EndList().Build()
        .Build();

    // Executes stream query
    auto resultStreamQuery = client.StreamExecuteQuery(query, TTxControl::NoTx(), parameters).GetValueSync();

    if (!resultStreamQuery.IsSuccess()) {
        return resultStreamQuery;
    }

    bool eos = false;

    while (!eos) {
        auto streamPart = resultStreamQuery.ReadNext().ExtractValueSync();

        if (!streamPart.IsSuccess()) {
            eos = true;
            if (!streamPart.EOS()) {
                return streamPart;
            }
            continue;
        }

        if (streamPart.HasResultSet()) {
            auto rs = streamPart.ExtractResultSet();
            resultSets.push_back(rs);
        }
    }
    return TStatus(EStatus::SUCCESS, NYql::TIssues());
}
```

{% include [multistep_transactions.md](steps/09_multistep_transactions.md) %}

Первый шаг — подготовка и выполнение первого запроса:

```c++
//! Shows usage of transactions consisting of multiple data queries with client logic between them.
static TAsyncExecuteQueryResult MultiStepTransaction(TSession session, const TString& path, ui64 seriesId, ui64 seasonId)
{
    auto query1 = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        DECLARE $seriesId AS Uint64;
        DECLARE $seasonId AS Uint64;

        SELECT first_aired AS from_date FROM seasons
        WHERE series_id = $seriesId AND season_id = $seasonId;
    )", path.c_str());

    auto params1 = TParamsBuilder()
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
    auto result = session.ExecuteQuery(
        query1,
        TTxControl::BeginTx(TTxSettings::SerializableRW()),
        params1);
    auto resultValue = result.GetValueSync();

    if (!resultValue.IsSuccess()) {
        return result;
    }
```

Для продолжения работы в рамках текущей транзакции необходимо получить текущий `transaction id`:

```c++
    // Get active transaction id
    auto tx = resultValue.GetTransaction();

    TResultSetParser parser(resultValue.GetResultSet(0));
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
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        DECLARE $seriesId AS Uint64;
        DECLARE $fromDate AS Uint64;
        DECLARE $toDate AS Uint64;

        SELECT season_id, episode_id, title, air_date FROM episodes
        WHERE series_id = $seriesId AND air_date >= $fromDate AND air_date <= $toDate;
    )", path.c_str());

    auto params2 = TParamsBuilder()
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
    result = session.ExecuteQuery(
        query2,
        TTxControl::Tx(tx->GetId()).CommitTx(),
        params2);

    resultValue = result.GetValueSync();

    return result;
}
```

Приведенные фрагменты кода при запуске выводят на консоль текст:

```bash
> MultiStep:
Episode 1, Season: 5, title: Grow Fast or Die Slow, Air date: Sun Mar 25, 2018
Episode 2, Season: 5, title: Reorientation, Air date: Sun Apr 01, 2018
Episode 3, Season: 5, title: Chief Operating Officer, Air date: Sun Apr 08, 2018
```

{% include [transaction_control.md](steps/10_transaction_control.md) %}

Фрагмент кода, демонстрирующий явное использование вызовов `BeginTransaction` и `tx.Commit()`:

**ВНИМАНИЕ**: Не стоит использовать без RetryQuery.
```c++
// Show usage of explicit Begin/Commit transaction control calls.
// In most cases it's better to use transaction control settings in ExecuteDataQuery calls instead
// to avoid additional hops to YDB cluster and allow more efficient execution of queries.
// WARNING: Do not use without RetryQuery!!!
static TStatus ExplicitTclTransaction(TQueryClient client, const TString& path, const TInstant& airDate) { 
    auto session = client.GetSession().GetValueSync().GetSession();
    auto beginResult = session.BeginTransaction(TTxSettings::SerializableRW()).GetValueSync();
    if (!beginResult.IsSuccess()) {
        return beginResult;
    }

    // Get newly created transaction id
    auto tx = beginResult.GetTransaction();

    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        DECLARE $airDate AS Date;

        UPDATE episodes SET air_date = CAST($airDate AS Uint16) WHERE title = "TBD";
    )", path.c_str());

    auto params = TParamsBuilder()
        .AddParam("$airDate")
            .Date(airDate)
            .Build()
        .Build();

    // Execute query.
    // Transaction control settings continues active transaction (tx)
    auto updateResult = session.ExecuteQuery(query,
        TTxControl::Tx(tx.GetId()),
        params).GetValueSync();

    if (!updateResult.IsSuccess()) {
        return updateResult;
    }
    // Commit active transaction (tx)
    return tx.Commit().GetValueSync();
}
```

