#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>

#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <thread>

namespace NKikimr {
namespace NStat {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

namespace {

void CreateTable(TTestEnv& env, const TString& databaseName, const TString& tableName, size_t rowCount) {
    ExecuteYqlScript(env, Sprintf(R"(
        CREATE TABLE `Root/%s/%s` (
            Key Uint64,
            Value Uint64,
            PRIMARY KEY (Key)
        );
    )", databaseName.c_str(), tableName.c_str()));

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
    ExecuteYqlScript(env, replace);
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

        runtime.SimulateSleep(TDuration::Seconds(1));
    }
}

} // namespace

Y_UNIT_TEST_SUITE(BasicStatistics) {

    Y_UNIT_TEST(Simple) {
        TTestEnv env(1, 1);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        CreateTable(env, "Database", "Table", 5);

        auto pathId = ResolvePathId(runtime, "/Root/Database/Table");
        ValidateRowCount(runtime, 1, pathId, 5);
    }

    Y_UNIT_TEST(TwoNodes) {
        TTestEnv env(1, 2);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database", 2);
        CreateTable(env, "Database", "Table", 5);

        auto pathId1 = ResolvePathId(runtime, "/Root/Database/Table");
        ValidateRowCount(runtime, 1, pathId1, 5);
        ValidateRowCount(runtime, 2, pathId1, 5);
    }

    Y_UNIT_TEST(TwoTables) {
        TTestEnv env(1, 1);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database");
        CreateTable(env, "Database", "Table1", 5);
        CreateTable(env, "Database", "Table2", 6);

        auto pathId1 = ResolvePathId(runtime, "/Root/Database/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Database/Table2");
        ValidateRowCount(runtime, 1, pathId1, 5);
        ValidateRowCount(runtime, 1, pathId2, 6);
    }

    Y_UNIT_TEST(TwoDatabases) {
        TTestEnv env(1, 2);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Database1", 1, false, "hdd1");
        CreateDatabase(env, "Database2", 1, false, "hdd2");
        CreateTable(env, "Database1", "Table1", 5);
        CreateTable(env, "Database2", "Table2", 6);

        auto pathId1 = ResolvePathId(runtime, "/Root/Database1/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Database2/Table2");
        ValidateRowCount(runtime, 2, pathId1, 5);
        ValidateRowCount(runtime, 1, pathId2, 6);
    }

    Y_UNIT_TEST(Serverless) {
        TTestEnv env(1, 1);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless", "/Root/Shared");
        CreateTable(env, "Serverless", "Table", 5);

        auto pathId = ResolvePathId(runtime, "/Root/Serverless/Table");
        ValidateRowCount(runtime, 1, pathId, 5);
    }

    Y_UNIT_TEST(TwoServerlessDbs) {
        TTestEnv env(1, 1);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless1", "/Root/Shared");
        CreateServerlessDatabase(env, "Serverless2", "/Root/Shared");
        CreateTable(env, "Serverless1", "Table1", 5);
        CreateTable(env, "Serverless2", "Table2", 6);

        auto pathId1 = ResolvePathId(runtime, "/Root/Serverless1/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Serverless2/Table2");
        ValidateRowCount(runtime, 1, pathId1, 5);
        ValidateRowCount(runtime, 1, pathId2, 6);
    }

    Y_UNIT_TEST(TwoServerlessTwoSharedDbs) {
        TTestEnv env(1, 2);

        auto& runtime = *env.GetServer().GetRuntime();

        CreateDatabase(env, "Shared1", 1, true, "hdd1");
        CreateDatabase(env, "Shared2", 1, true, "hdd2");
        CreateServerlessDatabase(env, "Serverless1", "/Root/Shared1");
        CreateServerlessDatabase(env, "Serverless2", "/Root/Shared2");
        CreateTable(env, "Serverless1", "Table1", 5);
        CreateTable(env, "Serverless2", "Table2", 6);

        auto pathId1 = ResolvePathId(runtime, "/Root/Serverless1/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Serverless2/Table2");
        ValidateRowCount(runtime, 2, pathId1, 5);
        ValidateRowCount(runtime, 1, pathId2, 6);
    }

}

} // NSysView
} // NKikimr
