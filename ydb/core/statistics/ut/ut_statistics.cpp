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

void CreateServerlessDatabase(TTestEnv& env, const TString& databaseName, TPathId resourcesDomainKey) {
    auto subdomain = GetSubDomainDeclareSettings(databaseName);
    subdomain.MutableResourcesDomainKey()->SetSchemeShard(resourcesDomainKey.OwnerId);
    subdomain.MutableResourcesDomainKey()->SetPathId(resourcesDomainKey.LocalPathId);
    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
        env.GetClient().CreateExtSubdomain("/Root", subdomain));

    env.GetTenants().Run("/Root/" + databaseName, 0);

    auto subdomainSettings = GetSubDomainDefaultSettings(databaseName, env.GetPools());
    subdomainSettings.SetExternalSchemeShard(true);
    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
        env.GetClient().AlterExtSubdomain("/Root", subdomainSettings));
}

void CreateTable(TTestEnv& env, const TString& databaseName, const TString& tableName, size_t rowCount) {
    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();

    auto result = session.ExecuteSchemeQuery(Sprintf(R"(
        CREATE TABLE `Root/%s/%s` (
            Key Uint64,
            Value Uint64,
            PRIMARY KEY (Key)
        );
    )", databaseName.c_str(), tableName.c_str())).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    TStringBuilder replace;
    replace << Sprintf("REPLACE INTO `Root/%s/%s` (Key, Value) VALUES ",
        databaseName.c_str(), tableName.c_str());
    for (ui32 i = 0; i < rowCount; ++i) {
        if (i > 0) {
            replace << ", ";
        }
        replace << Sprintf("(%uu, %uu)", i, i);
    }
    replace << ";";
    result = session.ExecuteDataQuery(replace, TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

TPathId ResolvePathId(TTestActorRuntime& runtime, const TString& path, TPathId* domainKey = nullptr) {
    auto sender = runtime.AllocateEdgeActor();

    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    using TEvRequest = TEvTxProxySchemeCache::TEvNavigateKeySet;
    using TEvResponse = TEvTxProxySchemeCache::TEvNavigateKeySetResult;

    auto request = std::make_unique<TNavigate>();
    auto& entry = request->ResultSet.emplace_back();
    entry.Path = SplitPath(path);
    entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    entry.ShowPrivatePath = true;
    runtime.Send(MakeSchemeCacheID(), sender, new TEvRequest(request.release()));

    auto ev = runtime.GrabEdgeEventRethrow<TEvResponse>(sender);
    UNIT_ASSERT(ev);
    UNIT_ASSERT(ev->Get());
    std::unique_ptr<TNavigate> response(ev->Get()->Request.Release());
    UNIT_ASSERT(response->ResultSet.size() == 1);
    auto& resultEntry = response->ResultSet[0];
    if (domainKey) {
        *domainKey = resultEntry.DomainInfo->DomainKey;
    }
    return resultEntry.TableId.PathId;
}

void ValidateRowCount(TTestActorRuntime& runtime, ui32 nodeIndex, TPathId pathId, size_t expectedRowCount) {
    auto statServiceId = NStat::MakeStatServiceID(runtime.GetNodeId(nodeIndex));
    ui64 rowCount = 0;
    while (rowCount == 0) {
        NStat::TRequest req;
        req.PathId = pathId;

        auto evGet = std::make_unique<TEvStatistics::TEvGetStatistics>();
        evGet->StatType = NStat::EStatType::SIMPLE;
        evGet->StatRequests.push_back(req);

        auto sender = runtime.AllocateEdgeActor(nodeIndex);
        runtime.Send(statServiceId, sender, evGet.release(), nodeIndex, true);
        auto evResult = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvGetStatisticsResult>(sender);

        UNIT_ASSERT(evResult);
        UNIT_ASSERT(evResult->Get());
        UNIT_ASSERT(evResult->Get()->StatResponses.size() == 1);

        auto rsp = evResult->Get()->StatResponses[0];
        auto stat = rsp.Simple;

        rowCount = stat.RowCount;

        if (rowCount != 0) {
            UNIT_ASSERT(stat.RowCount == expectedRowCount);
            break;
        }

        Sleep(TDuration::Seconds(5));
    }
}

} // namespace

Y_UNIT_TEST_SUITE(Statistics) {

    Y_UNIT_TEST(Simple) {
        TTestEnv env(1, 1);
        CreateDatabase(env, "Database");
        CreateTable(env, "Database", "Table", 5);

        auto& runtime = *env.GetServer().GetRuntime();
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table");

        ValidateRowCount(runtime, 1, pathId, 5);
    }

    Y_UNIT_TEST(TwoNodes) {
        TTestEnv env(1, 2);
        CreateDatabase(env, "Database", 2);
        CreateTable(env, "Database", "Table", 5);

        auto& runtime = *env.GetServer().GetRuntime();
        auto pathId1 = ResolvePathId(runtime, "/Root/Database/Table");

        ValidateRowCount(runtime, 1, pathId1, 5);
        ValidateRowCount(runtime, 2, pathId1, 5);
    }

    Y_UNIT_TEST(TwoTables) {
        TTestEnv env(1, 1);
        CreateDatabase(env, "Database");
        CreateTable(env, "Database", "Table1", 5);
        CreateTable(env, "Database", "Table2", 6);

        auto& runtime = *env.GetServer().GetRuntime();
        auto pathId1 = ResolvePathId(runtime, "/Root/Database/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Database/Table2");

        ValidateRowCount(runtime, 1, pathId1, 5);
        ValidateRowCount(runtime, 1, pathId2, 6);
    }

    Y_UNIT_TEST(TwoDatabases) {
        TTestEnv env(1, 2);
        CreateDatabase(env, "Database1");
        CreateDatabase(env, "Database2");
        CreateTable(env, "Database1", "Table1", 5);
        CreateTable(env, "Database2", "Table2", 6);

        auto& runtime = *env.GetServer().GetRuntime();
        auto pathId1 = ResolvePathId(runtime, "/Root/Database1/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Database2/Table2");

        ValidateRowCount(runtime, 1, pathId1, 5);
        ValidateRowCount(runtime, 2, pathId2, 6);
    }

    Y_UNIT_TEST(Serverless) {
        TTestEnv env(1, 1);
        CreateDatabase(env, "Shared");

        TPathId domainKey;
        auto& runtime = *env.GetServer().GetRuntime();
        ResolvePathId(runtime, "/Root/Shared", &domainKey);
        CreateServerlessDatabase(env, "Serverless", domainKey);
        CreateTable(env, "Serverless", "Table", 5);

        auto pathId = ResolvePathId(runtime, "/Root/Serverless/Table");

        ValidateRowCount(runtime, 1, pathId, 5);
    }

    Y_UNIT_TEST(TwoServerlessDbs) {
        TTestEnv env(1, 1);
        CreateDatabase(env, "Shared");

        TPathId domainKey;
        auto& runtime = *env.GetServer().GetRuntime();
        ResolvePathId(runtime, "/Root/Shared", &domainKey);
        CreateServerlessDatabase(env, "Serverless1", domainKey);
        CreateServerlessDatabase(env, "Serverless2", domainKey);
        CreateTable(env, "Serverless1", "Table1", 5);
        CreateTable(env, "Serverless2", "Table2", 6);

        auto pathId1 = ResolvePathId(runtime, "/Root/Serverless1/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Serverless2/Table2");

        ValidateRowCount(runtime, 1, pathId1, 5);
        ValidateRowCount(runtime, 1, pathId2, 6);
    }

    Y_UNIT_TEST(TwoServerlessTwoSharedDbs) {
        TTestEnv env(1, 2);
        CreateDatabase(env, "Shared1");
        CreateDatabase(env, "Shared2");

        TPathId domainKey1, domainKey2;
        auto& runtime = *env.GetServer().GetRuntime();
        ResolvePathId(runtime, "/Root/Shared1", &domainKey1);
        ResolvePathId(runtime, "/Root/Shared2", &domainKey2);
        CreateServerlessDatabase(env, "Serverless1", domainKey1);
        CreateServerlessDatabase(env, "Serverless2", domainKey2);

        CreateTable(env, "Serverless1", "Table1", 5);
        CreateTable(env, "Serverless2", "Table2", 6);

        auto pathId1 = ResolvePathId(runtime, "/Root/Serverless1/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Serverless2/Table2");

        ValidateRowCount(runtime, 1, pathId1, 5);
        ValidateRowCount(runtime, 2, pathId2, 6);
    }

}

} // NSysView
} // NKikimr
