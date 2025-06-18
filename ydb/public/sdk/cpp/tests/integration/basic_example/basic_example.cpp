#include "basic_example.h"

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <filesystem>
#include <format>

using namespace NYdb::NTable;

static std::string JoinPath(const std::string& basePath, const std::string& path) {
    if (basePath.empty()) {
        return path;
    }

    std::filesystem::path prefixPathSplit(basePath);
    prefixPathSplit /= path;

    return prefixPathSplit;
}

TRunArgs GetRunArgs() {
    std::string database = std::getenv("YDB_DATABASE");
    std::string endpoint = std::getenv("YDB_ENDPOINT");

    auto driverConfig = NYdb::TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(database)
        .SetAuthToken(std::getenv("YDB_TOKEN") ? std::getenv("YDB_TOKEN") : "");

    NYdb::TDriver driver(driverConfig);
    return {driver, database + "/" + std::string(std::getenv("YDB_TEST_ROOT")) + "/basic"};
}

///////////////////////////////////////////////////////////////////////////////

//! Creates sample tables with CrateTable API.
void CreateTables(TTableClient client, const std::string& path) {
    NYdb::NStatusHelpers::ThrowOnError(client.RetryOperationSync([path](TSession session) {
        auto seriesDesc = TTableBuilder()
            .AddNullableColumn("series_id", NYdb::EPrimitiveType::Uint64)
            .AddNullableColumn("title", NYdb::EPrimitiveType::Utf8)
            .AddNullableColumn("series_info", NYdb::EPrimitiveType::Utf8)
            .AddNullableColumn("release_date", NYdb::EPrimitiveType::Uint64)
            .SetPrimaryKeyColumn("series_id")
            .Build();

        return session.CreateTable(JoinPath(path, "series"), std::move(seriesDesc)).GetValueSync();
    }));

    NYdb::NStatusHelpers::ThrowOnError(client.RetryOperationSync([path](TSession session) {
        auto seasonsDesc = TTableBuilder()
            .AddNullableColumn("series_id", NYdb::EPrimitiveType::Uint64)
            .AddNullableColumn("season_id", NYdb::EPrimitiveType::Uint64)
            .AddNullableColumn("title", NYdb::EPrimitiveType::Utf8)
            .AddNullableColumn("first_aired", NYdb::EPrimitiveType::Uint64)
            .AddNullableColumn("last_aired", NYdb::EPrimitiveType::Uint64)
            .SetPrimaryKeyColumns({"series_id", "season_id"})
            .Build();

        return session.CreateTable(JoinPath(path, "seasons"), std::move(seasonsDesc)).GetValueSync();
    }));

    NYdb::NStatusHelpers::ThrowOnError(client.RetryOperationSync([path](TSession session) {
        auto episodesDesc = TTableBuilder()
            .AddNullableColumn("series_id", NYdb::EPrimitiveType::Uint64)
            .AddNullableColumn("season_id", NYdb::EPrimitiveType::Uint64)
            .AddNullableColumn("episode_id", NYdb::EPrimitiveType::Uint64)
            .AddNullableColumn("title", NYdb::EPrimitiveType::Utf8)
            .AddNullableColumn("air_date", NYdb::EPrimitiveType::Uint64)
            .SetPrimaryKeyColumns({"series_id", "season_id", "episode_id"})
            .Build();

        return session.CreateTable(JoinPath(path, "episodes"),
            std::move(episodesDesc)).GetValueSync();
    }));
}

///////////////////////////////////////////////////////////////////////////////

//! Fills sample tables with data in single parameterized data query.
NYdb::TStatus FillTableDataTransaction(TSession session, const std::string& path) {
    auto query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

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
    )", path);

    auto params = GetTablesDataParams();

    return session.ExecuteDataQuery(
        query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        params).GetValueSync();
}

//! Shows basic usage of YDB data queries and transactions.
static NYdb::TStatus SelectSimpleTransaction(TSession session, const std::string& path,
    std::optional<NYdb::TResultSet>& resultSet)
{
    auto query = std::format(R"(
        PRAGMA TablePathPrefix("{}");

        SELECT series_id, title, CAST(CAST(release_date AS Date) AS String) AS release_date
        FROM series
        WHERE series_id = 1
        ORDER BY series_id;
    )", path);

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
static NYdb::TStatus UpsertSimpleTransaction(TSession session, const std::string& path) {
    auto query = std::format(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("{}");

        UPSERT INTO episodes (series_id, season_id, episode_id, title) VALUES
            (2, 6, 1, "TBD");
    )", path);

    return session.ExecuteDataQuery(query,
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
}

//! Shows usage of parameters in data queries.
static NYdb::TStatus SelectWithParamsTransaction(TSession session, const std::string& path,
    uint64_t seriesId, uint64_t seasonId, std::optional<NYdb::TResultSet>& resultSet)
{
    auto query = std::format(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("{}");

        DECLARE $seriesId AS Uint64;
        DECLARE $seasonId AS Uint64;

        SELECT sa.title AS season_title, sr.title AS series_title
        FROM seasons AS sa
        INNER JOIN series AS sr
        ON sa.series_id = sr.series_id
        WHERE sa.series_id = $seriesId AND sa.season_id = $seasonId
        ORDER BY season_title, series_title;
    )", path);

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
static NYdb::TStatus PreparedSelectTransaction(TSession session, const std::string& path,
    uint64_t seriesId, uint64_t seasonId, uint64_t episodeId, std::optional<NYdb::TResultSet>& resultSet)
{
    // Once prepared, query data is stored in the session and identified by QueryId.
    // Local query cache is used to keep track of queries, prepared in current session.
    auto query = std::format(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("{}");

        DECLARE $seriesId AS Uint64;
        DECLARE $seasonId AS Uint64;
        DECLARE $episodeId AS Uint64;

        SELECT *
        FROM episodes
        WHERE series_id = $seriesId AND season_id = $seasonId AND episode_id = $episodeId
        ORDER BY season_id, episode_id;
    )", path);

    // Prepare query or get result from query cache
    auto prepareResult = session.PrepareDataQuery(query).GetValueSync();
    if (!prepareResult.IsSuccess()) {
        return prepareResult;
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
static NYdb::TStatus MultiStepTransaction(TSession session, const std::string& path, uint64_t seriesId, uint64_t seasonId,
    std::optional<NYdb::TResultSet>& resultSet)
{
    auto query1 = std::format(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("{}");

        DECLARE $seriesId AS Uint64;
        DECLARE $seasonId AS Uint64;

        SELECT first_aired AS from_date FROM seasons
        WHERE series_id = $seriesId AND season_id = $seasonId
        ORDER BY from_date;
    )", path);

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

    NYdb::TResultSetParser parser(result.GetResultSet(0));
    parser.TryNextRow();
    auto date = parser.ColumnParser("from_date").GetOptionalUint64();

    // Perform some client logic on returned values
    auto userFunc = [] (const TInstant fromDate) {
        return fromDate + TDuration::Days(15);
    };

    TInstant fromDate = TInstant::Days(*date);
    TInstant toDate = userFunc(fromDate);

    // Construct next query based on the results of client logic
    auto query2 = std::format(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("{}");

        DECLARE $seriesId AS Uint64;
        DECLARE $fromDate AS Uint64;
        DECLARE $toDate AS Uint64;

        SELECT season_id, episode_id, title, air_date FROM episodes
        WHERE series_id = $seriesId AND air_date >= $fromDate AND air_date <= $toDate
        ORDER BY season_id, episode_id;
    )", path);

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
static NYdb::TStatus ExplicitTclTransaction(TSession session, const std::string& path, const TInstant& airDate) {
    auto beginResult = session.BeginTransaction(TTxSettings::SerializableRW()).GetValueSync();
    if (!beginResult.IsSuccess()) {
        return beginResult;
    }

    // Get newly created transaction id
    auto tx = beginResult.GetTransaction();

    auto query = std::format(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("{}");

        DECLARE $airDate AS Date;

        UPDATE episodes SET air_date = CAST($airDate AS Uint16) WHERE title = "TBD";
    )", path);

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

static NYdb::TStatus ScanQuerySelect(TTableClient client, const std::string& path, std::vector<NYdb::TResultSet>& vectorResultSet) {    
    vectorResultSet.clear();
    auto query = std::format(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("{}");

        DECLARE $series AS List<UInt64>;

        SELECT series_id, season_id, title, CAST(CAST(first_aired AS Date) AS String) AS first_aired
        FROM seasons
        WHERE series_id IN $series
        ORDER BY season_id;
    )", path);

    auto parameters = NYdb::TParamsBuilder()
        .AddParam("$series")
        .BeginList()
            .AddListItem().Uint64(1)
            .AddListItem().Uint64(10)
        .EndList().Build()
        .Build();

    // Executes scan query
    auto resultScanQuery = client.StreamExecuteScanQuery(query, parameters).GetValueSync();

    if (!resultScanQuery.IsSuccess()) {
        return resultScanQuery;
    }

    bool eos = false;

    while (!eos) {
        auto streamPart = resultScanQuery.ReadNext().ExtractValueSync();

        if (!streamPart.IsSuccess()) {
            eos = true;
            if (!streamPart.EOS()) {
                return streamPart;
            }
            continue;
        }

        if (streamPart.HasResultSet()) {
            auto rs = streamPart.ExtractResultSet();
            vectorResultSet.push_back(rs);
        }
    }
    return NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues());
}

///////////////////////////////////////////////////////////////////////////////

NYdb::TResultSet SelectSimple(TTableClient client, const std::string& path) {
    std::optional<NYdb::TResultSet> resultSet;
    NYdb::NStatusHelpers::ThrowOnError(client.RetryOperationSync([path, &resultSet](TSession session) {
        return SelectSimpleTransaction(session, path, resultSet);
    }));

    return resultSet.value();
}

void UpsertSimple(TTableClient client, const std::string& path) {
    NYdb::NStatusHelpers::ThrowOnError(client.RetryOperationSync([path](TSession session) {
        return UpsertSimpleTransaction(session, path);
    }));
}

NYdb::TResultSet SelectWithParams(TTableClient client, const std::string& path) {
    std::optional<NYdb::TResultSet> resultSet;
    NYdb::NStatusHelpers::ThrowOnError(client.RetryOperationSync([path, &resultSet](TSession session) {
        return SelectWithParamsTransaction(session, path, 2, 3, resultSet);
    }));

    return resultSet.value();
}

NYdb::TResultSet PreparedSelect(TTableClient client, const std::string& path, ui32 seriesId, ui32 seasonId, ui32 episodeId) {
    std::optional<NYdb::TResultSet> resultSet;
    NYdb::NStatusHelpers::ThrowOnError(client.RetryOperationSync([path, seriesId, seasonId, episodeId, &resultSet](TSession session) {
        return PreparedSelectTransaction(session, path, seriesId, seasonId, episodeId, resultSet);
    }));

    return resultSet.value();
}

NYdb::TResultSet MultiStep(TTableClient client, const std::string& path) {
    std::optional<NYdb::TResultSet> resultSet;
    NYdb::NStatusHelpers::ThrowOnError(client.RetryOperationSync([path, &resultSet](TSession session) {
        return MultiStepTransaction(session, path, 2, 5, resultSet);
    }));

    return resultSet.value();
}

void ExplicitTcl(TTableClient client, const std::string& path) {
    NYdb::NStatusHelpers::ThrowOnError(client.RetryOperationSync([path](TSession session) {
        return ExplicitTclTransaction(session, path, TInstant());
    }));
}

std::vector<NYdb::TResultSet> ScanQuerySelect(TTableClient client, const std::string& path) {
    std::vector<NYdb::TResultSet> resultSets;
    NYdb::NStatusHelpers::ThrowOnError(client.RetryOperationSync([path, &resultSets](TTableClient& client) {
        return ScanQuerySelect(client, path, resultSets);
    }));

    return resultSets;
}
