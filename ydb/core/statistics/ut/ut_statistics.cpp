#include "ut_common.h"

#include <library/cpp/actors/testlib/test_runtime.h>

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

void CreateDatabase(TTestEnv& env, const TString& databaseName) {
    auto subdomain = GetSubDomainDeclareSettings(databaseName);
    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
        env.GetClient().CreateExtSubdomain("/Root", subdomain));

    env.GetTenants().Run("/Root/" + databaseName, 1);

    auto subdomainSettings = GetSubDomainDefaultSettings(databaseName, env.GetPools());
    subdomainSettings.SetExternalSchemeShard(true);
    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
        env.GetClient().AlterExtSubdomain("/Root", subdomainSettings));
}

void CreateTable(TTestEnv& env, const TString& databaseName, const TString& tableName) {
    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();

    auto result = session.ExecuteSchemeQuery(Sprintf(R"(
        CREATE TABLE `Root/%s/%s` (
            Key Uint64,
            Value String,
            PRIMARY KEY (Key)
        );
    )", databaseName.c_str(), tableName.c_str())).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    result = session.ExecuteDataQuery(Sprintf(R"(
        REPLACE INTO `Root/%s/%s` (Key, Value) VALUES
            (1u, "A"),
            (2u, "B"),
            (3u, "C");
    )", databaseName.c_str(), tableName.c_str()), TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void CreateAll(TTestEnv& env) {
    CreateDatabase(env, "Database");
    CreateTable(env, "Database", "Table");
}

std::unique_ptr<NSchemeCache::TSchemeCacheNavigate> Navigate(TTestActorRuntime& runtime,
    const TActorId& sender, const TString& path, NSchemeCache::TSchemeCacheNavigate::EOp op)
{
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    using TEvRequest = TEvTxProxySchemeCache::TEvNavigateKeySet;
    using TEvResponse = TEvTxProxySchemeCache::TEvNavigateKeySetResult;

    auto request = MakeHolder<TNavigate>();
    auto& entry = request->ResultSet.emplace_back();
    entry.Path = SplitPath(path);
    entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
    entry.Operation = op;
    entry.ShowPrivatePath = true;
    runtime.Send(MakeSchemeCacheID(), sender, new TEvRequest(request.Release()));

    auto ev = runtime.GrabEdgeEventRethrow<TEvResponse>(sender);
    UNIT_ASSERT(ev);
    UNIT_ASSERT(ev->Get());
    return std::unique_ptr<TNavigate>(ev->Get()->Request.Release());
}

} // namespace

Y_UNIT_TEST_SUITE(Statistics) {

    Y_UNIT_TEST(Simple) {
        TTestEnv env(1, 1);
        CreateAll(env);

        auto& runtime = *env.GetServer().GetRuntime();
        auto navigate = Navigate(runtime, runtime.AllocateEdgeActor(),
            "/Root/Database/Table",
            NSchemeCache::TSchemeCacheNavigate::EOp::OpTable);
        const auto& entry = navigate->ResultSet[0];

        auto statServiceId = NStat::MakeStatServiceID(runtime.GetNodeId(0));

        ui64 rowCount = 0;
        while (rowCount == 0) {
            NStat::TRequest req;
            req.StatType = NStat::EStatType::SIMPLE;
            req.PathId = entry.TableId.PathId;

            auto evGet = std::make_unique<NStat::TEvStatistics::TEvGetStatistics>();
            evGet->StatRequests.push_back(req);

            auto sender = runtime.AllocateEdgeActor();
            runtime.Send(statServiceId, sender, evGet.release());
            auto evResult = runtime.GrabEdgeEventRethrow<NStat::TEvStatistics::TEvGetStatisticsResult>(sender);

            UNIT_ASSERT(evResult);
            UNIT_ASSERT(evResult->Get());
            UNIT_ASSERT(evResult->Get()->StatResponses.size() == 1);

            auto rsp = evResult->Get()->StatResponses[0];
            auto stat = std::get<NKikimr::NStat::TStatSimple>(rsp.Statistics);

            rowCount = stat.RowCount;

            if (rowCount != 0) {
                UNIT_ASSERT(stat.RowCount == 3);
                break;
            }

            Sleep(TDuration::Seconds(5));
        }
    }

}

} // NSysView
} // NKikimr
