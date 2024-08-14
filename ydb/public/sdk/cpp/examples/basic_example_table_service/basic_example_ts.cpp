#include "basic_example_ts.h"

#include <util/folder/pathsplit.h>
#include <util/string/printf.h>

using namespace NYdb;
using namespace NYdb::NTable;

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

static void PrintStatus(const TStatus& status) {
    Cerr << "Status: " << status.GetStatus() << Endl;
    status.GetIssues().PrintTo(Cerr);
}

static TString JoinPath(const TString& basePath, const TString& path) {
    if (basePath.empty()) {
        return path;
    }

    TPathSplitUnix prefixPathSplit(basePath);
    prefixPathSplit.AppendComponent(path);

    return prefixPathSplit.Reconstruct();
}

///////////////////////////////////////////////////////////////////////////////

//! Creates sample tables with CrateTable API.
static void CreateTables(TTableClient client, const TString& path) {
    ThrowOnError(client.RetryOperationSync([path](TSession session) {
        auto seriesDesc = TTableBuilder()
            .AddNullableColumn("series_id", EPrimitiveType::Uint64)
            .AddNullableColumn("title", EPrimitiveType::Utf8)
            .AddNullableColumn("series_info", EPrimitiveType::Utf8)
            .AddNullableColumn("release_date", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumn("series_id")
            .Build();

        return session.CreateTable(JoinPath(path, "series"), std::move(seriesDesc)).GetValueSync();
    }));

    ThrowOnError(client.RetryOperationSync([path](TSession session) {
        auto seasonsDesc = TTableBuilder()
            .AddNullableColumn("series_id", EPrimitiveType::Uint64)
            .AddNullableColumn("season_id", EPrimitiveType::Uint64)
            .AddNullableColumn("title", EPrimitiveType::Utf8)
            .AddNullableColumn("first_aired", EPrimitiveType::Uint64)
            .AddNullableColumn("last_aired", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumns({"series_id", "season_id"})
            .Build();

        return session.CreateTable(JoinPath(path, "seasons"), std::move(seasonsDesc)).GetValueSync();
    }));

    ThrowOnError(client.RetryOperationSync([path](TSession session) {
        auto episodesDesc = TTableBuilder()
            .AddNullableColumn("series_id", EPrimitiveType::Uint64)
            .AddNullableColumn("season_id", EPrimitiveType::Uint64)
            .AddNullableColumn("episode_id", EPrimitiveType::Uint64)
            .AddNullableColumn("title", EPrimitiveType::Utf8)
            .AddNullableColumn("air_date", EPrimitiveType::Uint64)
            .SetPrimaryKeyColumns({"series_id", "season_id", "episode_id"})
            .Build();

        return session.CreateTable(JoinPath(path, "episodes"),
            std::move(episodesDesc)).GetValueSync();
    }));
}

//! Describe existing table.
static void DescribeTable(TTableClient client, const TString& path, const TString& name) {
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
}

///////////////////////////////////////////////////////////////////////////////

//! Fills sample tables with data in single parameterized data query.
static TStatus FillTableDataTransaction(TSession session, const TString& path) {
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

    return session.ExecuteDataQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        params).GetValueSync();
}

//! Shows basic usage of YDB data queries and transactions.
static TStatus SelectSimpleTransaction(TSession session, const TString& path,
    TMaybe<TResultSet>& resultSet)
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

    // Executes data query with specified transaction control settings.
    auto result = session.ExecuteDataQuery(query, txControl).GetValueSync();

    if (result.IsSuccess()) {
        // Index of result set corresponds to its order in YQL query
        resultSet = result.GetResultSet(0);
    }

    return result;
}

//! Shows basic usage of mutating operations.
static TStatus UpsertSimpleTransaction(TSession session, const TString& path) {
    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        UPSERT INTO episodes (series_id, season_id, episode_id, title) VALUES
            (2, 6, 1, "TBD");
    )", path.c_str());

    return session.ExecuteDataQuery(query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
}

//! Shows usage of parameters in data queries.
static TStatus SelectWithParamsTransaction(TSession session, const TString& path,
    ui64 seriesId, ui64 seasonId, TMaybe<TResultSet>& resultSet)
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
        params).GetValueSync();

    if (result.IsSuccess()) {
        resultSet = result.GetResultSet(0);
    }

    return result;
}

//! Shows usage of prepared queries.
static TStatus PreparedSelectTransaction(TSession session, const TString& path,
    ui64 seriesId, ui64 seasonId, ui64 episodeId, TMaybe<TResultSet>& resultSet)
{
    // Once prepared, query data is stored in the session and identified by QueryId.
    // Local query cache is used to keep track of queries, prepared in current session.
    auto query = Sprintf(R"(
        --!syntax_v1
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

    if (!prepareResult.IsQueryFromCache()) {
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
        params).GetValueSync();

    if (result.IsSuccess()) {
        resultSet = result.GetResultSet(0);
    }

    return result;
}

//! Shows usage of transactions consisting of multiple data queries with client logic between them.
static TStatus MultiStepTransaction(TSession session, const TString& path, ui64 seriesId, ui64 seasonId,
    TMaybe<TResultSet>& resultSet)
{
    auto query1 = Sprintf(R"(
        --!syntax_v1
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
        params1).GetValueSync();

    if (!result.IsSuccess()) {
        return result;
    }

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
        params2).GetValueSync();

    if (result.IsSuccess()) {
        resultSet = result.GetResultSet(0);
    }

    return result;
}

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
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        DECLARE $airDate AS Date;

        UPDATE episodes SET air_date = CAST($airDate AS Uint16) WHERE title = "TBD";
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
        params).GetValueSync();

    if (!updateResult.IsSuccess()) {
        return updateResult;
    }

    // Commit active transaction (tx)
    return tx.Commit().GetValueSync();
}

///////////////////////////////////////////////////////////////////////////////

void SelectSimple(TTableClient client, const TString& path) {
    TMaybe<TResultSet> resultSet;
    ThrowOnError(client.RetryOperationSync([path, &resultSet](TSession session) {
        return SelectSimpleTransaction(session, path, resultSet);
    }));

    TResultSetParser parser(*resultSet);
    if (parser.TryNextRow()) {
        Cout << "> SelectSimple:" << Endl << "Series"
            << ", Id: " << parser.ColumnParser("series_id").GetOptionalUint64()
            << ", Title: " << parser.ColumnParser("title").GetOptionalUtf8()
            << ", Release date: " << parser.ColumnParser("release_date").GetOptionalString()
            << Endl;
    }
}

void UpsertSimple(TTableClient client, const TString& path) {
    ThrowOnError(client.RetryOperationSync([path](TSession session) {
        return UpsertSimpleTransaction(session, path);
    }));
}

void SelectWithParams(TTableClient client, const TString& path) {
    TMaybe<TResultSet> resultSet;
    ThrowOnError(client.RetryOperationSync([path, &resultSet](TSession session) {
        return SelectWithParamsTransaction(session, path, 2, 3, resultSet);
    }));

    TResultSetParser parser(*resultSet);
    if (parser.TryNextRow()) {
        Cout << "> SelectWithParams:" << Endl << "Season"
            << ", Title: " << parser.ColumnParser("season_title").GetOptionalUtf8()
            << ", Series title: " << parser.ColumnParser("series_title").GetOptionalUtf8()
            << Endl;
    }
}

void PreparedSelect(TTableClient client, const TString& path, ui32 seriesId, ui32 seasonId, ui32 episodeId) {
    TMaybe<TResultSet> resultSet;
    ThrowOnError(client.RetryOperationSync([path, seriesId, seasonId, episodeId, &resultSet](TSession session) {
        return PreparedSelectTransaction(session, path, seriesId, seasonId, episodeId, resultSet);
    }));

    TResultSetParser parser(*resultSet);
    if (parser.TryNextRow()) {
        auto airDate = TInstant::Days(*parser.ColumnParser("air_date").GetOptionalUint64());

        Cout << "> PreparedSelect:" << Endl << "Episode " << parser.ColumnParser("episode_id").GetOptionalUint64()
            << ", Title: " << parser.ColumnParser("title").GetOptionalUtf8()
            << ", Air date: " << airDate.FormatLocalTime("%a %b %d, %Y")
            << Endl;
    }
}

void MultiStep(TTableClient client, const TString& path) {
    TMaybe<TResultSet> resultSet;
    ThrowOnError(client.RetryOperationSync([path, &resultSet](TSession session) {
        return MultiStepTransaction(session, path, 2, 5, resultSet);
    }));

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

void ExplicitTcl(TTableClient client, const TString& path) {
    ThrowOnError(client.RetryOperationSync([path](TSession session) {
        return ExplicitTclTransaction(session, path, TInstant::Now());
    }));
}

void ScanQuerySelect(TTableClient client, const TString& path) {
    auto query = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        DECLARE $series AS List<UInt64>;

        SELECT series_id, season_id, title, CAST(CAST(first_aired AS Date) AS String) AS first_aired
        FROM seasons
        WHERE series_id IN $series
    )", path.c_str());

    auto parameters = TParamsBuilder()
        .AddParam("$series")
        .BeginList()
            .AddListItem().Uint64(1)
            .AddListItem().Uint64(10)
        .EndList().Build()
        .Build();

    // Executes scan query
    auto result = client.StreamExecuteScanQuery(query, parameters).GetValueSync();

    if (!result.IsSuccess()) {
        Cerr << "ScanQuery execution failure: " << result.GetIssues().ToString() << Endl;
        return;
    }

    bool eos = false;
    Cout << "> ScanQuerySelect:" << Endl;
    while (!eos) {
        auto streamPart = result.ReadNext().ExtractValueSync();

        if (!streamPart.IsSuccess()) {
            eos = true;
            if (!streamPart.EOS()) {
                Cerr << "ScanQuery execution failure: " << streamPart.GetIssues().ToString() << Endl;
            }
            continue;
        }

        if (streamPart.HasResultSet()) {
            auto rs = streamPart.ExtractResultSet();
            auto columns = rs.GetColumnsMeta();

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
}

///////////////////////////////////////////////////////////////////////////////

bool Run(const TDriver& driver, const TString& path) {
    TTableClient client(driver);

    try {
        CreateTables(client, path);

        DescribeTable(client, path, "series");

        ThrowOnError(client.RetryOperationSync([path](TSession session) {
            return FillTableDataTransaction(session, path);
        }));

        SelectSimple(client, path);
        UpsertSimple(client, path);

        SelectWithParams(client, path);

        PreparedSelect(client, path, 2, 3, 7);
        PreparedSelect(client, path, 2, 3, 8);

        MultiStep(client, path);

        ExplicitTcl(client, path);

        PreparedSelect(client, path, 2, 6, 1);

        ScanQuerySelect(client, path);
    }
    catch (const TYdbErrorException& e) {
        Cerr << "Execution failed due to fatal error:" << Endl;
        PrintStatus(e.Status);
        return false;
    }

    return true;
}
