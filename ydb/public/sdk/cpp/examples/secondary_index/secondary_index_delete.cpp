#include "secondary_index.h"

using namespace NLastGetopt;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NStatusHelpers;

////////////////////////////////////////////////////////////////////////////////

static TStatus DeleteSeries(TSession& session, const std::string& prefix, uint64_t seriesId, uint64_t& deletedCount) {
    auto queryText = std::format(R"(
        --!syntax_v1
        PRAGMA TablePathPrefix("{}");

        DECLARE $seriesId AS Uint64;

        -- Simulate a DESC index by inverting views using max(uint64)-views
        $maxUint64 = 0xffffffffffffffff;

        $data = (
            SELECT series_id, ($maxUint64 - views) AS rev_views
            FROM series
            WHERE series_id = $seriesId
        );

        DELETE FROM series
        ON SELECT series_id FROM $data;

        DELETE FROM series_rev_views
        ON SELECT rev_views, series_id FROM $data;

        SELECT COUNT(*) AS cnt FROM $data;
    )", prefix);

    auto prepareResult = session.PrepareDataQuery(queryText).ExtractValueSync();
    if (!prepareResult.IsSuccess()) {
        return prepareResult;
    }

    auto query = prepareResult.GetQuery();

    auto params = query.GetParamsBuilder()
        .AddParam("$seriesId")
            .Uint64(seriesId)
            .Build()
        .Build();

    auto result = query.Execute(
        TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
        std::move(params)).ExtractValueSync();

    if (result.IsSuccess()) {
        auto parser = result.GetResultSetParser(0);
        if (parser.TryNextRow()) {
            deletedCount = parser.ColumnParser(0).GetUint64();
        }
    }

    return result;
}

int RunDeleteSeries(TDriver& driver, const std::string& prefix, int argc, char** argv) {
    TOpts opts = TOpts::Default();

    uint64_t seriesId;

    opts.AddLongOption("id", "Series id").Required().RequiredArgument("NUM")
        .StoreResult(&seriesId);

    TOptsParseResult res(&opts, argc, argv);

    uint64_t deletedCount = 0;
    TTableClient client(driver);
    ThrowOnError(client.RetryOperationSync([&](TSession session) -> TStatus {
        return DeleteSeries(session, prefix, seriesId, deletedCount);
    }));

    std::cout << "Deleted " << deletedCount << " rows" << std::endl;
    return 0;
}
