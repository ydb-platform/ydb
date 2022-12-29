#include "secondary_index.h"

#include <util/charset/utf8.h>

using namespace NLastGetopt;
using namespace NYdb;
using namespace NYdb::NTable;

////////////////////////////////////////////////////////////////////////////////

static void ParseSeries(TVector<TSeries>& results, TResultSetParser&& parser) {
    results.clear();
    while (parser.TryNextRow()) {
        auto& series = results.emplace_back();
        series.SeriesId = *parser.ColumnParser(0).GetOptionalUint64();
        series.Title = *parser.ColumnParser(1).GetOptionalUtf8();
        series.SeriesInfo = *parser.ColumnParser(2).GetOptionalUtf8();
        series.ReleaseDate = TInstant::Days(*parser.ColumnParser(3).GetOptionalUint32());
        series.Views = *parser.ColumnParser(4).GetOptionalUint64();
    }
}

static TStatus ListByViews(
        TVector<TSeries>& results,
        TSession& session,
        const TString& prefix,
        ui64 limit,
        ui64 lastSeriesId,
        ui64 lastViews)
{
    auto queryText = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%1$s");

        DECLARE $limit AS Uint64;
        DECLARE $lastSeriesId AS Uint64;
        DECLARE $lastViews AS Uint64;

        -- Simulate a DESC index by inverting views using max(uint64)-views
        $maxUint64 = 0xffffffffffffffff;
        $lastRevViews = $maxUint64 - $lastViews;

        $filterRaw = (
            SELECT rev_views, series_id
            FROM series_rev_views
            WHERE rev_views = $lastRevViews AND series_id > $lastSeriesId
            ORDER BY rev_views, series_id
            LIMIT $limit
            UNION ALL
            SELECT rev_views, series_id
            FROM series_rev_views
            WHERE rev_views > $lastRevViews
            ORDER BY rev_views, series_id
            LIMIT $limit
        );

        -- $filterRaw may have more than $limit rows
        $filter = (
            SELECT rev_views, series_id
            FROM $filterRaw
            ORDER BY rev_views, series_id
            LIMIT $limit
        );

        SELECT t2.series_id AS series_id, title, series_info, release_date, views
        FROM $filter AS t1
        INNER JOIN series AS t2 USING (series_id)
        ORDER BY views DESC, series_id ASC;
    )", prefix.data());

    auto prepareResult = session.PrepareDataQuery(queryText).ExtractValueSync();
    if (!prepareResult.IsSuccess()) {
        return prepareResult;
    }

    auto query = prepareResult.GetQuery();

    auto params = query.GetParamsBuilder()
        .AddParam("$limit")
            .Uint64(limit)
            .Build()
        .AddParam("$lastSeriesId")
            .Uint64(lastSeriesId)
            .Build()
        .AddParam("$lastViews")
            .Uint64(lastViews)
            .Build()
        .Build();

    auto result = query.Execute(
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        std::move(params)).ExtractValueSync();

    if (result.IsSuccess()) {
        ParseSeries(results, result.GetResultSetParser(0));
    }

    return result;
}

static TStatus ListByViews(
        TVector<TSeries>& results,
        TSession& session,
        const TString& prefix,
        ui64 limit)
{
    auto queryText = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%1$s");

        DECLARE $limit AS Uint64;

        $filter = (
            SELECT rev_views, series_id
            FROM series_rev_views
            ORDER BY rev_views, series_id
            LIMIT $limit
        );

        SELECT t2.series_id AS series_id, title, series_info, release_date, views
        FROM $filter AS t1
        INNER JOIN series AS t2 USING (series_id)
        ORDER BY views DESC, series_id ASC;
    )", prefix.data());

    auto prepareResult = session.PrepareDataQuery(queryText).ExtractValueSync();
    if (!prepareResult.IsSuccess()) {
        return prepareResult;
    }

    auto query = prepareResult.GetQuery();

    auto params = query.GetParamsBuilder()
        .AddParam("$limit")
            .Uint64(limit)
            .Build()
        .Build();

    auto result = query.Execute(
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        std::move(params)).ExtractValueSync();

    if (result.IsSuccess()) {
        ParseSeries(results, result.GetResultSetParser(0));
    }

    return result;
}

static TStatus ListById(
        TVector<TSeries>& results,
        TSession& session,
        const TString& prefix,
        ui64 limit,
        ui64 lastSeriesId)
{
    auto queryText = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%1$s");

        DECLARE $limit AS Uint64;
        DECLARE $lastSeriesId AS Uint64;

        SELECT series_id, title, series_info, release_date, views
        FROM series
        WHERE series_id > $lastSeriesId
        ORDER BY series_id
        LIMIT $limit;
    )", prefix.data());

    auto prepareResult = session.PrepareDataQuery(queryText).ExtractValueSync();
    if (!prepareResult.IsSuccess()) {
        return prepareResult;
    }

    auto query = prepareResult.GetQuery();

    auto params = query.GetParamsBuilder()
        .AddParam("$limit")
            .Uint64(limit)
            .Build()
        .AddParam("$lastSeriesId")
            .Uint64(lastSeriesId)
            .Build()
        .Build();

    auto result = query.Execute(
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        std::move(params)).ExtractValueSync();

    if (result.IsSuccess()) {
        ParseSeries(results, result.GetResultSetParser(0));
    }

    return result;
}

static TStatus ListById(
        TVector<TSeries>& results,
        TSession& session,
        const TString& prefix,
        ui64 limit)
{
    auto queryText = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%1$s");

        DECLARE $limit AS Uint64;

        SELECT series_id, title, series_info, release_date, views
        FROM series
        ORDER BY series_id
        LIMIT $limit;
    )", prefix.data());

    auto prepareResult = session.PrepareDataQuery(queryText).ExtractValueSync();
    if (!prepareResult.IsSuccess()) {
        return prepareResult;
    }

    auto query = prepareResult.GetQuery();

    auto params = query.GetParamsBuilder()
        .AddParam("$limit")
            .Uint64(limit)
            .Build()
        .Build();

    auto result = query.Execute(
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        std::move(params)).ExtractValueSync();

    if (result.IsSuccess()) {
        ParseSeries(results, result.GetResultSetParser(0));
    }

    return result;
}

int RunListSeries(TDriver& driver, const TString& prefix, int argc, char** argv) {
    TOpts opts = TOpts::Default();

    bool byViews = false;
    ui64 limit = 10;
    ui64 lastSeriesId = -1;
    ui64 lastViews = -1;

    opts.AddLongOption("by-views", "Sort by views").NoArgument().SetFlag(&byViews);
    opts.AddLongOption("limit", "Maximum number of rows").Optional().RequiredArgument("NUM")
        .StoreResult(&limit);
    opts.AddLongOption("last-id", "Resume from this last series id").Optional().RequiredArgument("NUM")
        .StoreResult(&lastSeriesId);
    opts.AddLongOption("last-views", "Resume from this last series views").Optional().RequiredArgument("NUM")
        .StoreResult(&lastViews);

    TOptsParseResult res(&opts, argc, argv);

    TVector<TSeries> results;
    TTableClient client(driver);
    ThrowOnError(client.RetryOperationSync([&](TSession session) -> TStatus {
        if (byViews) {
            if (res.Has("last-id") && res.Has("last-views")) {
                return ListByViews(results, session, prefix, limit, lastSeriesId, lastViews);
            } else {
                return ListByViews(results, session, prefix, limit);
            }
        } else {
            if (res.Has("last-id")) {
                return ListById(results, session, prefix, limit, lastSeriesId);
            } else {
                return ListById(results, session, prefix, limit);
            }
        }
    }));

    size_t rows = results.size() + 1;
    TVector<TString> columns[5];
    for (size_t i = 0; i < 5; ++i) {
        columns[i].reserve(rows);
    }
    columns[0].push_back("series_id");
    columns[1].push_back("title");
    columns[2].push_back("series_info");
    columns[3].push_back("release_date");
    columns[4].push_back("views");
    for (const auto& result : results) {
        columns[0].push_back(TStringBuilder() << result.SeriesId);
        columns[1].push_back(TStringBuilder() << result.Title);
        columns[2].push_back(TStringBuilder() << result.SeriesInfo);
        columns[3].push_back(TStringBuilder() << result.ReleaseDate.FormatGmTime("%Y-%m-%d"));
        columns[4].push_back(TStringBuilder() << result.Views);
    }
    size_t widths[5] = { 0 };
    for (size_t i = 0; i < 5; ++i) {
        for (const auto& value : columns[i]) {
            widths[i] = Max(widths[i], GetNumberOfUTF8Chars(value) + 2);
        }
    }
    auto printLine = [&]() {
        Cout << '+';
        for (size_t i = 0; i < 5; ++i) {
            for (size_t k = 0; k < widths[i]; ++k) {
                Cout << '-';
            }
            Cout << '+';
        }
        Cout << Endl;
    };
    auto printRow = [&](size_t row) {
        Cout << '|';
        for (size_t i = 0; i < 5; ++i) {
            Cout << ' ' << columns[i][row];
            size_t printed = 1 + GetNumberOfUTF8Chars(columns[i][row]);
            while (printed < widths[i]) {
                Cout << ' ';
                ++printed;
            }
            Cout << '|';
        }
        Cout << Endl;
    };

    printLine();
    printRow(0);
    printLine();
    if (rows > 1) {
        for (size_t row = 1; row < rows; ++row) {
            printRow(row);
        }
        printLine();
    }

    return 0;
}
