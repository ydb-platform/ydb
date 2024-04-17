#include "ut_common.h"

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/stat_service.h>

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

namespace NKikimr {
namespace NStat {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

namespace {

void CreateUniformTable(TTestEnv& env, const TString& databaseName, const TString& tableName) {
    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();

    auto result = session.ExecuteSchemeQuery(Sprintf(R"(
        CREATE TABLE `Root/%s/%s` (
            Key Uint64,
            Value Uint64,
            PRIMARY KEY (Key)
        )
        WITH ( UNIFORM_PARTITIONS = 4 );
    )", databaseName.c_str(), tableName.c_str())).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    TStringBuilder replace;
    replace << Sprintf("REPLACE INTO `Root/%s/%s` (Key, Value) VALUES ",
        databaseName.c_str(), tableName.c_str());
    for (ui32 i = 0; i < 4; ++i) {
        if (i > 0) {
            replace << ", ";
        }
        ui64 value = 4000000000000000000ull * (i + 1);
        replace << Sprintf("(%" PRIu64 "ul, %" PRIu64 "ul)", value, value);
    }
    replace << ";";
    result = session.ExecuteDataQuery(replace, TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

TPathId ResolvePathId(TTestActorRuntime& runtime, const TString& path, ui64* tabletId) {
    auto sender = runtime.AllocateEdgeActor();

    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    using TEvRequest = TEvTxProxySchemeCache::TEvNavigateKeySet;
    using TEvResponse = TEvTxProxySchemeCache::TEvNavigateKeySetResult;

    auto request = std::make_unique<TNavigate>();
    auto& entry = request->ResultSet.emplace_back();
    entry.Path = SplitPath(path);
    entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
    entry.Operation = TNavigate::EOp::OpPath;
    entry.ShowPrivatePath = true;
    runtime.Send(MakeSchemeCacheID(), sender, new TEvRequest(request.release()));

    auto ev = runtime.GrabEdgeEventRethrow<TEvResponse>(sender);
    UNIT_ASSERT(ev);
    UNIT_ASSERT(ev->Get());
    std::unique_ptr<TNavigate> response(ev->Get()->Request.Release());
    UNIT_ASSERT(response->ResultSet.size() == 1);
    auto& resultEntry = response->ResultSet[0];
    if (resultEntry.DomainInfo->Params.HasStatisticsAggregator()) {
        *tabletId = resultEntry.DomainInfo->Params.GetStatisticsAggregator();
    }
    return resultEntry.TableId.PathId;
}

} // namespace

Y_UNIT_TEST_SUITE(StatisticsAggregator) {

    Y_UNIT_TEST(ScanOneTable) {
        TTestEnv env(1, 1);
        CreateDatabase(env, "Database");
        CreateUniformTable(env, "Database", "Table");

        auto& runtime = *env.GetServer().GetRuntime();

        ui64 tabletId = 0;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", &tabletId);

        auto ev = std::make_unique<TEvStatistics::TEvScanTable>();
        auto& record = ev->Record;
        PathIdFromPathId(pathId, record.MutablePathId());

        auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(tabletId, sender, ev.release());
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvScanTableResponse>(sender);

        auto statServiceId = NStat::MakeStatServiceID(runtime.GetNodeId(1));
        NStat::TRequest req;
        req.PathId = pathId;
        req.ColumnName = "Key";

        auto evGet = std::make_unique<TEvStatistics::TEvGetStatistics>();
        evGet->StatType = NStat::EStatType::COUNT_MIN_SKETCH;
        evGet->StatRequests.push_back(req);

        auto sender2 = runtime.AllocateEdgeActor(1);
        runtime.Send(statServiceId, sender2, evGet.release(), 1, true);
        auto evResult = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvGetStatisticsResult>(sender2);

        UNIT_ASSERT(evResult);
        UNIT_ASSERT(evResult->Get());
        UNIT_ASSERT(evResult->Get()->StatResponses.size() == 1);

        auto rsp = evResult->Get()->StatResponses[0];
        auto stat = rsp.CountMinSketch;
        UNIT_ASSERT(rsp.Success);
        UNIT_ASSERT(stat.CountMin);

        for (ui32 i = 0; i < 4; ++i) {
            ui64 value = 4000000000000000000ull * (i + 1);
            auto probe = stat.CountMin->Probe((const char *)&value, sizeof(ui64));
            UNIT_ASSERT_VALUES_EQUAL(probe, 1);
        }
    }
}

} // NStat
} // NKikimr
