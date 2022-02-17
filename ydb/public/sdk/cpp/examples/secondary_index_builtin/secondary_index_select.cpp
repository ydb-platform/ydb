#include "secondary_index.h"

#include <util/string/printf.h>

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NLastGetopt;

TStatus SelectSeriesWithViews(TSession session, const TString& path, TVector<TSeries>& selectResult, ui64 minViews) {
    auto queryText = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        DECLARE $minViews AS Uint64;

        SELECT series_id, title, info, release_date, views, uploaded_user_id
        FROM `series` VIEW views_index
        WHERE views >= $minViews
    )", path.c_str());

    auto prepareResult = session.PrepareDataQuery(queryText).ExtractValueSync();
    if (!prepareResult.IsSuccess()) {
        return prepareResult;
    }

    auto query = prepareResult.GetQuery();
    auto params = query.GetParamsBuilder()
        .AddParam("$minViews")
            .Uint64(minViews)
            .Build()
        .Build();

    auto result = query.Execute(TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), std::move(params))
        .ExtractValueSync();

    if (result.IsSuccess()) {
        ParseSelectSeries(selectResult,  result.GetResultSetParser(0));
    }

    return result;
}

int Select(TDriver& driver, const TString& path, int argc, char **argv) {

    TOpts opts = TOpts::Default();

    ui64 minViews = 0;
    opts.AddLongOption("min-views", "Series with views greater than").Required().RequiredArgument("NUM")
        .StoreResult(&minViews);

    TOptsParseResult res(&opts, argc, argv);
    TTableClient client(driver);

    TVector<TSeries> selectResult;
    ThrowOnError(client.RetryOperationSync([path, minViews, &selectResult](TSession session) {
        return SelectSeriesWithViews(session, path, selectResult, minViews);
    }));

    for (auto& item: selectResult) {
        Cout << item.SeriesId << ' ' << item.Title << ' ' << item.Info << ' '
            << item.ReleaseDate <<  ' ' << item.Views << ' ' << item.UploadedUserId <<  Endl;
    }

    return 0;
}


