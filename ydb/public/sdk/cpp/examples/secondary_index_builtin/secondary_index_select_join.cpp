#include "secondary_index.h"

#include <util/string/printf.h>

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NLastGetopt;

TStatus SelectSeriesWithUserName(TSession session, const TString& path,
            TVector<TSeries>& selectResult, const TString& name) {

    auto queryText = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%s");

        DECLARE $userName AS Utf8;

        SELECT t1.series_id, t1.title, t1.info, t1.release_date, t1.views, t1.uploaded_user_id
        FROM `series` VIEW users_index AS t1
        INNER JOIN `users` VIEW name_index AS t2
        ON t1.uploaded_user_id == t2.user_id
        WHERE t2.name == $userName;
    )", path.c_str());

    auto prepareResult = session.PrepareDataQuery(queryText).ExtractValueSync();
    if (!prepareResult.IsSuccess()) {
        return prepareResult;
    }

    auto query = prepareResult.GetQuery();
    auto params = query.GetParamsBuilder()
        .AddParam("$userName")
            .Utf8(name)
            .Build()
        .Build();

    auto result = query.Execute(TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), std::move(params))
            .ExtractValueSync();

    if (result.IsSuccess()) {
        ParseSelectSeries(selectResult,  result.GetResultSetParser(0));
    }

    return result;
}

int SelectJoin(TDriver& driver, const TString& path, int argc, char **argv) {

    TOpts opts = TOpts::Default();

    TString name;
    opts.AddLongOption("name", "User name").Required().RequiredArgument("TYPE")
        .StoreResult(&name);

    TOptsParseResult res(&opts, argc, argv);
    TTableClient client(driver);

    TVector<TSeries> selectResult;

    ThrowOnError(client.RetryOperationSync([path, &selectResult, name](TSession session) {
        return SelectSeriesWithUserName(session, path, selectResult, name);
    }));

    for (auto& item : selectResult) {
        Cout << item.SeriesId << ' ' << item.Title << Endl;
    }

    return 0;
}

