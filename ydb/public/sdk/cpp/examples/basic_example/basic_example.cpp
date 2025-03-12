#include "basic_example.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/retry/retry.h>

#include <util/string/cast.h>

#include <format>

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NYdb::NStatusHelpers;

template <class T>
std::string OptionalToString(const std::optional<T>& opt) {
    if (opt.has_value()) {
        return std::to_string(opt.value());
    }
    return "(NULL)";
}

template <>
std::string OptionalToString<std::string>(const std::optional<std::string>& opt) {
    if (opt.has_value()) {
        return opt.value();
    }
    return "(NULL)";
}

///////////////////////////////////////////////////////////////////////////////

static void CreateTables(TQueryClient client) {
    //! Creates sample tables with the ExecuteQuery method
    ThrowOnError(client.RetryQuerySync([](TSession session) {
        auto query = R"(
            CREATE TABLE series (
                series_id Uint64,
                title Utf8,
                series_info Utf8,
                release_date Uint64,
                PRIMARY KEY (series_id)
            );
        )";
        return session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
    }));

    ThrowOnError(client.RetryQuerySync([](TSession session) {
        auto query = R"(
            CREATE TABLE seasons (
                series_id Uint64,
                season_id Uint64,
                title Utf8,
                first_aired Uint64,
                last_aired Uint64,
                PRIMARY KEY (series_id, season_id)
            );
        )";
        return session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
    }));

    ThrowOnError(client.RetryQuerySync([](TSession session) {
        auto query = R"(
            CREATE TABLE episodes (
                series_id Uint64,
                season_id Uint64,
                episode_id Uint64,
                title Utf8,
                air_date Uint64,
                PRIMARY KEY (series_id, season_id, episode_id)
            );
        )";
        return session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
    }));
}

///////////////////////////////////////////////////////////////////////////////

static void DropTables(TQueryClient client) {
    ThrowOnError(client.RetryQuerySync([](TSession session) {
        auto query = R"(
            DROP TABLE series;
        )";
        return session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
    }));

    ThrowOnError(client.RetryQuerySync([](TSession session) {
        auto query = R"(
            DROP TABLE seasons;
        )";
        return session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
    }));
    
    ThrowOnError(client.RetryQuerySync([](TSession session) {
        auto query = R"(
            DROP TABLE episodes;
        )";
        return session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
    }));
}

///////////////////////////////////////////////////////////////////////////////

void FillTableData(TQueryClient client) {
    ThrowOnError(client.RetryQuerySync([](TSession session) {
        auto query = R"(
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
        )";

        auto params = GetTablesDataParams();

        return session.ExecuteQuery(
            query,
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
            params).GetValueSync();
    }));
}

void SelectSimple(TQueryClient client) {
    std::optional<TResultSet> resultSet;
    ThrowOnError(client.RetryQuerySync([&resultSet](TSession session) {
        auto query = R"(
            SELECT series_id, title, CAST(release_date AS Date) AS release_date
            FROM series
            WHERE series_id = 1;
        )";

        auto txControl =
            // Begin a new transaction with SerializableRW mode
            TTxControl::BeginTx(TTxSettings::SerializableRW())
            // Commit the transaction at the end of the query
            .CommitTx();

        auto result = session.ExecuteQuery(query, txControl).GetValueSync();
        if (!result.IsSuccess()) {
            return result;
        }
        resultSet = result.GetResultSet(0);
        return result;
    }));

    TResultSetParser parser(*resultSet);
    while (parser.TryNextRow()) {
        std::cout << "> SelectSimple:" << std::endl << "Series"
            << ", Id: " << OptionalToString(parser.ColumnParser("series_id").GetOptionalUint64())
            << ", Title: " << OptionalToString(parser.ColumnParser("title").GetOptionalUtf8())
            << ", Release date: " << parser.ColumnParser("release_date").GetOptionalDate()->FormatLocalTime("%Y-%m-%d")
            << std::endl;
    }
}

void UpsertSimple(TQueryClient client) {
    ThrowOnError(client.RetryQuerySync([](TSession session) {
        auto query = R"(
            UPSERT INTO episodes (series_id, season_id, episode_id, title) VALUES
                (2, 6, 1, "TBD");
        )";

        return session.ExecuteQuery(query,
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
    }));
}

void SelectWithParams(TQueryClient client) {
    std::optional<TResultSet> resultSet;
    ThrowOnError(client.RetryQuerySync([&resultSet](TSession session) {
        ui64 seriesId = 2;
        ui64 seasonId = 3;
        auto query = R"(
            DECLARE $seriesId AS Uint64;
            DECLARE $seasonId AS Uint64;

            SELECT sa.title AS season_title, sr.title AS series_title
            FROM seasons AS sa
            INNER JOIN series AS sr
            ON sa.series_id = sr.series_id
            WHERE sa.series_id = $seriesId AND sa.season_id = $seasonId;
        )";

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
            params).GetValueSync();
        
        if (!result.IsSuccess()) {
            return result;
        }
        resultSet = result.GetResultSet(0);
        return result;
    }));

    TResultSetParser parser(*resultSet);
    if (parser.TryNextRow()) {
        std::cout << "> SelectWithParams:" << std::endl << "Season"
            << ", Title: " << OptionalToString(parser.ColumnParser("season_title").GetOptionalUtf8())
            << ", Series title: " << OptionalToString(parser.ColumnParser("series_title").GetOptionalUtf8())
            << std::endl;
    }
}

void MultiStep(TQueryClient client) {
    TMaybe<TResultSet> resultSet;
    ThrowOnError(client.RetryQuerySync([&resultSet](TSession session) {
        ui64 seriesId = 2;
        ui64 seasonId = 5;
        auto query1 = R"(
            DECLARE $seriesId AS Uint64;
            DECLARE $seasonId AS Uint64;

            SELECT first_aired AS from_date FROM seasons
            WHERE series_id = $seriesId AND season_id = $seasonId;
        )";

        auto params1 = TParamsBuilder()
            .AddParam("$seriesId")
                .Uint64(seriesId)
                .Build()
            .AddParam("$seasonId")
                .Uint64(seasonId)
                .Build()
            .Build();

        // Execute the first query to retrieve the required values for the client.
        // Transaction control settings do not set the CommitTx flag, allowing the transaction to remain active
        // after query execution.
        auto result = session.ExecuteQuery(
            query1,
            TTxControl::BeginTx(TTxSettings::SerializableRW()),
            params1);

        auto resultValue = result.GetValueSync();

        if (!resultValue.IsSuccess()) {
            return resultValue;
        }

        // Get the active transaction id
        auto txId = resultValue.GetTransaction()->GetId();
        
        // Processing the request result
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
        auto query2 = R"(
            DECLARE $seriesId AS Uint64;
            DECLARE $fromDate AS Uint64;
            DECLARE $toDate AS Uint64;

            SELECT season_id, episode_id, title, air_date FROM episodes
            WHERE series_id = $seriesId AND air_date >= $fromDate AND air_date <= $toDate;
        )";

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

        // Execute the second query.
        // The transaction control settings continue the active transaction (tx)
        // and commit it at the end of the second query execution.
        auto result2 = session.ExecuteQuery(
            query2,
            TTxControl::Tx(txId).CommitTx(),
            params2).GetValueSync();
        
        if (!result2.IsSuccess()) {
            return result2;
        }
        resultSet = result2.GetResultSet(0);
        return result2;
    })); // The end of the retried lambda

    TResultSetParser parser(*resultSet);
    std::cout << "> MultiStep:" << std::endl;
    while (parser.TryNextRow()) {
        auto airDate = TInstant::Days(*parser.ColumnParser("air_date").GetOptionalUint64());

        std::cout << "Episode " << OptionalToString(parser.ColumnParser("episode_id").GetOptionalUint64())
            << ", Season: " << OptionalToString(parser.ColumnParser("season_id").GetOptionalUint64())
            << ", Title: " << OptionalToString(parser.ColumnParser("title").GetOptionalUtf8())
            << ", Air date: " << airDate.FormatLocalTime("%a %b %d, %Y")
            << std::endl;
    }
}

void ExplicitTcl(TQueryClient client) {
    // Demonstrate the use of explicit Begin and Commit transaction control calls.
    // In most cases, it's preferable to use transaction control settings within ExecuteDataQuery calls instead, 
    // as this avoids additional hops to the YDB cluster and allows for more efficient query execution.
    ThrowOnError(client.RetryQuerySync([](TQueryClient client) -> TStatus {
        auto airDate = TInstant::Now();
        auto session = client.GetSession().GetValueSync().GetSession();
        auto beginResult = session.BeginTransaction(TTxSettings::SerializableRW()).GetValueSync();
        if (!beginResult.IsSuccess()) {
            return beginResult;
        }

        // Get newly created transaction id
        auto tx = beginResult.GetTransaction();

        auto query = R"(
            DECLARE $airDate AS Date;

            UPDATE episodes SET air_date = CAST($airDate AS Uint16) WHERE title = "TBD";
        )";

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
    }));
}

void StreamQuerySelect(TQueryClient client) {
    std::cout << "> StreamQuery:" << std::endl;

    ThrowOnError(client.RetryQuerySync([](TQueryClient client) -> TStatus {
        auto query = R"(
            DECLARE $series AS List<UInt64>;

            SELECT series_id, season_id, title, CAST(first_aired AS Date) AS first_aired
            FROM seasons
            WHERE series_id IN $series
            ORDER BY season_id;
        )";

        auto paramsBuilder = TParamsBuilder();
        auto& listParams = paramsBuilder
                                    .AddParam("$series")
                                    .BeginList();
        
        for (auto x : {1, 10}) {
            listParams.AddListItem().Uint64(x);
        }
                
        auto parameters = listParams
                                .EndList()
                                .Build()
                                .Build();

        // Executes stream query
        auto resultStreamQuery = client.StreamExecuteQuery(query, TTxControl::NoTx(), parameters).GetValueSync();

        if (!resultStreamQuery.IsSuccess()) {
            return resultStreamQuery;
        }

        // Iterates over results
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

            // It is possible to duplicate lines in the output stream due to an external retryer.
            if (streamPart.HasResultSet()) {
                auto rs = streamPart.ExtractResultSet();
                TResultSetParser parser(rs);
                while (parser.TryNextRow()) {
                    std::cout << "Season"
                            << ", SeriesId: " << OptionalToString(parser.ColumnParser("series_id").GetOptionalUint64())
                            << ", SeasonId: " << OptionalToString(parser.ColumnParser("season_id").GetOptionalUint64())
                            << ", Title: " << OptionalToString(parser.ColumnParser("title").GetOptionalUtf8())
                            << ", Air date: " << parser.ColumnParser("first_aired").GetOptionalDate()->FormatLocalTime("%Y-%m-%d")
                            << std::endl;
                }
            }
        }
        return TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues());
    }));

}


///////////////////////////////////////////////////////////////////////////////

bool Run(const TDriver& driver) {
    TQueryClient client(driver);

    try {
        CreateTables(client);

        FillTableData(client);

        SelectSimple(client);
        UpsertSimple(client);

        SelectWithParams(client);

        MultiStep(client);

        ExplicitTcl(client);

        StreamQuerySelect(client);

        DropTables(client);
    }
    catch (const TYdbErrorException& e) {
        std::cerr << "Execution failed due to fatal error: " << e.what() << std::endl;
        return false;
    }

    return true;
}
