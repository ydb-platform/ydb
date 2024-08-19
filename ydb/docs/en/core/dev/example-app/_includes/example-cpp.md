# App in C++

This page contains a detailed description of the code of a [test app](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp/examples/basic_example) that is available as part of the {{ ydb-short-name }} [C++SDK](https://github.com/ydb-platform/ydb/tree/main/ydb/public/sdk/cpp).

{% include [init.md](steps/01_init.md) %}

App code snippet for driver initialization:

```c++
    auto connectionParams = TConnectionsParams()
        .SetEndpoint(endpoint)
        .SetDatabase(database)
        .SetAuthToken(GetEnv("YDB_TOKEN"));

    TDriver driver(connectionParams);
```

App code snippet for creating a client:

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

Code snippet for data insert/update:

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


To execute YQL queries, use the `ExecuteQuery` method.
The SDK lets you explicitly control the execution of transactions and configure the transaction execution mode using the `TTxControl` class.

In the code snippet below, the transaction is started with the `TTxControl::BeginTx` method. With `TTxSettings`, set the `SerializableRW` transaction execution mode. When all the queries in the transaction are completed, the transaction is automatically completed by explicitly setting `CommitTx()`. The `query` described using the YQL syntax is passed to the `ExecuteQuery` method for execution.


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

The `TResultSetParser` class is used for processing query execution results.  

The code snippet below shows how to process query results using the `parser` object:

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

The given code snippet prints the following text to the console at startup:

```bash
> SelectSimple:
series, Id: 1, title: IT Crowd, Release date: 2006-02-03
```

{% include [param_queries.md](steps/06_param_queries.md) %}

The code snippet shows the use of parameterized queries and the `TParamsBuilder` to generate parameters and pass them to the `ExecuteQuery`method:

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

The given code snippet prints the following text to the console at startup:

```bash
> SelectWithParams:
Season, title: Season 3, series title: Silicon Valley
Finished preparing query: PreparedSelectTransaction
```

## Stream queries {#stream-query}

Making a stream query that results in a data stream. Streaming lets you read an unlimited number of rows and amount of data.

**WARNING**: Do not use without RetryQuery.

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

The first step is to prepare and execute the first query:

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

To continue working within the current transaction, you need to get the current `transaction id`:

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

The next step is to create the next query that uses the results of code execution on the client side:

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

The given code snippets output the following text to the console at startup:

```bash
> MultiStep:
Episode 1, Season: 5, title: Grow Fast or Die Slow, Air date: Sun Mar 25, 2018
Episode 2, Season: 5, title: Reorientation, Air date: Sun Apr 01, 2018
Episode 3, Season: 5, title: Chief Operating Officer, Air date: Sun Apr 08, 2018
```

{% include [transaction_control.md](steps/10_transaction_control.md) %}

Code snippet for `BeginTransaction` and `tx.Commit()` calls:

**WARNING**: Do not use without RetryQuery.

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
