#include "basic_example_qs.h"

#include <ydb/public/sdk/cpp/client/ydb_retry/retry.h>

#include <util/folder/pathsplit.h>
#include <util/string/printf.h>

using namespace NYdb;
using namespace NYdb::NQuery;

class TYdbErrorException : public yexception {
public:
    TYdbErrorException(const TStatus& status)
        : Status(status) {}

    TStatus Status;
};

static void ThrowOnError(const TStatus& status) {
    if (!status.IsSuccess()) {
        throw TYdbErrorException(status) << status;
    }
}

static void ThrowOnError(const TAsyncExecuteQueryResult& result) {
    auto status = result.GetValueSync();
    ThrowOnError(status);
}

static void ThrowOnError(const TAsyncExecuteQueryResult& result, TMaybe<TResultSet>& resultSet) {
    ThrowOnError(result);
    resultSet = result.GetValueSync().GetResultSet(0);
}

static void PrintStatus(const TStatus& status) {
    Cerr << "Status: " << status.GetStatus() << Endl;
    status.GetIssues().PrintTo(Cerr);
}

///////////////////////////////////////////////////////////////////////////////

static void CreateTables(TQueryClient client, const TString& path) {
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

    ThrowOnError(client.RetryQuery([path](TSession session) {
        auto query = Sprintf(R"(
            PRAGMA TablePathPrefix("%s");
            CREATE TABLE seasons (
                series_id Uint64,
                season_id Uint64,
                title Utf8,
                first_aired Uint64,
                last_aired Uint64,
                PRIMARY KEY (series_id, season_id)
            );
        )", path.c_str());
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }));

    ThrowOnError(client.RetryQuery([path](TSession session) {
        auto query = Sprintf(R"(
            PRAGMA TablePathPrefix("%s");
            CREATE TABLE episodes (
                series_id Uint64,
                season_id Uint64,
                episode_id Uint64,
                title Utf8,
                air_date Uint64,
                PRIMARY KEY (series_id, season_id, episode_id)
            );
        )", path.c_str());
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }));
}

static void DropTables(TQueryClient client, const TString& path) {
    ThrowOnError(client.RetryQuery([path](TSession session) {
        auto query = Sprintf(R"(
            PRAGMA TablePathPrefix("%s");
            DROP TABLE series;
        )", path.c_str());
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }));

    ThrowOnError(client.RetryQuery([path](TSession session) {
        auto query = Sprintf(R"(
            PRAGMA TablePathPrefix("%s");
            DROP TABLE seasons;
        )", path.c_str());
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }));
    
    ThrowOnError(client.RetryQuery([path](TSession session) {
        auto query = Sprintf(R"(
            PRAGMA TablePathPrefix("%s");
            DROP TABLE episodes;
        )", path.c_str());
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }));
}

// ///////////////////////////////////////////////////////////////////////////////

// //! Fills sample tables with data in single parameterized data query.
static TAsyncExecuteQueryResult FillTableDataTransaction(TSession session, const TString& path) {
    auto query = Sprintf(R"(
        PRAGMA TablePathPrefix("%s");

        DECLARE $seriesData AS List<Struct<
            series_id: Uint64,
            title: Utf8,
            series_info: Utf8,
            release_date: Date>>;

        DECLARE $seasonsData AS List<Struct<
            series_id: Uint64,
            season_id: Uint64,
            title: Utf8,
            first_aired: Date,
            last_aired: Date>>;

        DECLARE $episodesData AS List<Struct<
            series_id: Uint64,
            season_id: Uint64,
            episode_id: Uint64,
            title: Utf8,
            air_date: Date>>;

        REPLACE INTO series
        SELECT
            series_id,
            title,
            series_info,
            CAST(release_date AS Uint16) AS release_date
        FROM AS_TABLE($seriesData);

        REPLACE INTO seasons
        SELECT
            series_id,
            season_id,
            title,
            CAST(first_aired AS Uint16) AS first_aired,
            CAST(last_aired AS Uint16) AS last_aired
        FROM AS_TABLE($seasonsData);

        REPLACE INTO episodes
        SELECT
            series_id,
            season_id,
            episode_id,
            title,
            CAST(air_date AS Uint16) AS air_date
        FROM AS_TABLE($episodesData);
    )", path.c_str());

    auto params = GetTablesDataParams();

    return session.ExecuteQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        params);
}

//! Shows basic usage of YDB data queries and transactions.
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

// //! Shows basic usage of mutating operations.
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

//! Shows usage of parameters in data queries.
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

// Show usage of explicit Begin/Commit transaction control calls.
// In most cases it's better to use transaction control settings in ExecuteDataQuery calls instead
// to avoid additional hops to YDB cluster and allow more efficient execution of queries.
static TStatus ExplicitTclTransaction(TQueryClient client, const TString& path, const TInstant& airDate) { 
    auto session = client.GetSession().GetValueSync().GetSession();
    auto beginResult = session.BeginTransaction(TTxSettings::SerializableRW());
    auto beginResultValue = beginResult.GetValueSync();
    if (!beginResultValue.IsSuccess()) {
        return beginResultValue;
    }

    // Get newly created transaction id
    auto tx = beginResultValue.GetTransaction();

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
        params);
    auto updateResultValue = updateResult.GetValueSync();

    if (!updateResultValue.IsSuccess()) {
        return updateResultValue;
    }
    // Commit active transaction (tx)
    return tx.Commit().GetValueSync();
}

static TStatus StreamQuerySelectTransaction(TQueryClient client, const TString& path) {
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
    auto resultStreamQuery = client.StreamExecuteQuery(query, TTxControl::NoTx(), parameters);
    auto resultStreamQueryValue = resultStreamQuery.GetValueSync();

    if (!resultStreamQueryValue.IsSuccess()) {
        return resultStreamQueryValue;
    }

    bool eos = false;

    while (!eos) {
        auto streamPart = resultStreamQueryValue.ReadNext().ExtractValueSync();

        if (!streamPart.IsSuccess()) {
            eos = true;
            if (!streamPart.EOS()) {
                return streamPart;
            }
            continue;
        }

        Cout << "> StreamQuery:" << Endl;
        if (streamPart.HasResultSet()) {
            auto rs = streamPart.ExtractResultSet();

            TResultSetParser parser(rs);
            while (parser.TryNextRow()) {
                Cout << "Season"
                     << ", SeriesId: " << parser.ColumnParser("series_id").GetOptionalUint64()
                     << ", SeasonId: " << parser.ColumnParser("season_id").GetOptionalUint64()
                     << ", Title: " << parser.ColumnParser("title").GetOptionalUtf8()
                     << ", Air date: " << parser.ColumnParser("first_aired").GetOptionalString()
                     << Endl;
            }
        }
    }
    return TStatus(EStatus::SUCCESS, NYql::TIssues());
}

///////////////////////////////////////////////////////////////////////////////

void MultiStep(TQueryClient client, const TString& path) {
    TMaybe<TResultSet> resultSet;
    ThrowOnError(client.RetryQuery([path](TSession session) {
        return MultiStepTransaction(session, path, 2, 5);
    }), resultSet);

    TResultSetParser parser(*resultSet);
    Cout << "> MultiStep:" << Endl;
    while (parser.TryNextRow()) {
        auto airDate = TInstant::Days(*parser.ColumnParser("air_date").GetOptionalUint64());

        Cout << "Episode " << parser.ColumnParser("episode_id").GetOptionalUint64()
            << ", Season: " << parser.ColumnParser("season_id").GetOptionalUint64()
            << ", Title: " << parser.ColumnParser("title").GetOptionalUtf8()
            << ", Air date: " << airDate.FormatLocalTime("%a %b %d, %Y")
            << Endl;
    }
}

void SelectWithParams(TQueryClient client, const TString& path) {
    TMaybe<TResultSet> resultSet;
    ThrowOnError(client.RetryQuery([path](TSession session) {
        return SelectWithParamsTransaction(session, path, 2, 3);
    }), resultSet);

    TResultSetParser parser(*resultSet);
    if (parser.TryNextRow()) {
        Cout << "> SelectWithParams:" << Endl << "Season"
            << ", Title: " << parser.ColumnParser("season_title").GetOptionalUtf8()
            << ", Series title: " << parser.ColumnParser("series_title").GetOptionalUtf8()
            << Endl;
    }
}

void UpsertSimple(TQueryClient client, const TString& path) {
    ThrowOnError(client.RetryQuery([path](TSession session) {
        return UpsertSimpleTransaction(session, path);
    }));
}

void SelectSimple(TQueryClient client, const TString& path) {
    TMaybe<TResultSet> resultSet;
    ThrowOnError(client.RetryQuery([path](TSession session) {
        return SelectSimpleTransaction(session, path);
    }), resultSet);

    TResultSetParser parser(*resultSet);
    if (parser.TryNextRow()) {
        Cout << "> SelectSimple:" << Endl << "Series"
            << ", Id: " << parser.ColumnParser("series_id").GetOptionalUint64()
            << ", Title: " << parser.ColumnParser("title").GetOptionalUtf8()
            << ", Release date: " << parser.ColumnParser("release_date").GetOptionalString()
            << Endl;
    }
}


void FillTableData(TQueryClient client, const TString& path) {
    ThrowOnError(client.RetryQuery([path](TSession session) {
            return FillTableDataTransaction(session, path);
        }));
}

///////////////////////////////////////////////////////////////////////////////

bool Run(const TDriver& driver, const TString& path) {
    TQueryClient client(driver);

    try {
        CreateTables(client, path);

        FillTableData(client, path);

        SelectSimple(client, path);
        UpsertSimple(client, path);

        SelectWithParams(client, path);

        MultiStep(client, path);

        ExplicitTclTransaction(client, path, TInstant::Now());

        StreamQuerySelectTransaction(client, path);

        DropTables(client, path);
    }
    catch (const TYdbErrorException& e) {
        Cerr << "Execution failed due to fatal error:" << Endl;
        PrintStatus(e.Status);
        return false;
    }

    return true;
}
