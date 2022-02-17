#include "secondary_index.h"

using namespace NLastGetopt;
using namespace NYdb;
using namespace NYdb::NTable;

////////////////////////////////////////////////////////////////////////////////

int RunUpdateViews(TDriver& driver, const TString& prefix, int argc, char** argv) {
    TOpts opts = TOpts::Default();

    ui64 seriesId;
    ui64 newViews;

    opts.AddLongOption("id", "Series id").Required().RequiredArgument("NUM")
        .StoreResult(&seriesId);
    opts.AddLongOption("views", "New views").Required().RequiredArgument("NUM")
        .StoreResult(&newViews);

    TOptsParseResult res(&opts, argc, argv);

    TString queryText = Sprintf(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("%1$s");

        DECLARE $seriesId AS Uint64;
        DECLARE $newViews AS Uint64;

        -- Simulate a DESC index by inverting views using max(uint64)-views
        $maxUint64 = 0xffffffffffffffff;
        $newRevViews = $maxUint64 - $newViews;

        $data = (
            SELECT series_id, ($maxUint64 - views) AS old_rev_views
            FROM series
            WHERE series_id = $seriesId
        );

        UPSERT INTO series
        SELECT series_id, $newViews AS views FROM $data;

        DELETE FROM series_rev_views
        ON SELECT old_rev_views AS rev_views, series_id FROM $data;

        UPSERT INTO series_rev_views
        SELECT $newRevViews AS rev_views, series_id FROM $data;

        SELECT COUNT(*) AS cnt FROM $data;
    )", prefix.data());

    ui64 updatedCount = 0;

    TTableClient client(driver);
    ThrowOnError(client.RetryOperationSync([&](TSession session) -> TStatus {
        auto prepareResult = session.PrepareDataQuery(queryText).ExtractValueSync();
        if (!prepareResult.IsSuccess()) {
            return prepareResult;
        }

        auto query = prepareResult.GetQuery();

        auto params = query.GetParamsBuilder()
            .AddParam("$seriesId")
                .Uint64(seriesId)
                .Build()
            .AddParam("$newViews")
                .Uint64(newViews)
                .Build()
            .Build();

        auto result = query.Execute(
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
            std::move(params)).ExtractValueSync();

        if (result.IsSuccess()) {
            auto parser = result.GetResultSetParser(0);
            if (parser.TryNextRow()) {
                updatedCount = parser.ColumnParser(0).GetUint64();
            }
        }

        return result;
    }));

    Cout << "Updated " << updatedCount << " rows" << Endl;
    return 0;
}
