#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/core/testlib/actors/block_events.h>

#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/tx/datashard/datashard.h>

namespace NKikimr {
namespace NStat {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

namespace {

void FillTable(TTestEnv& env, const TString& databaseName, const TString& tableName, size_t rowCount) {
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

void CreateTable(TTestEnv& env, const TString& databaseName, const TString& tableName, size_t rowCount) {
    ExecuteYqlScript(env, Sprintf(R"(
        CREATE TABLE `Root/%s/%s` (
            Key Uint64,
            Value Uint64,
            PRIMARY KEY (Key)
        );
    )", databaseName.c_str(), tableName.c_str()));
    FillTable(env, databaseName, tableName, rowCount);
}

void CreateTableWithGlobalIndex(TTestEnv& env, const TString& databaseName, const TString& tableName, size_t rowCount) {
    ExecuteYqlScript(env, Sprintf(R"(
        CREATE TABLE `Root/%s/%s` (
            Key Uint64,
            Value Uint64,
            INDEX ValueIndex GLOBAL ON ( Value ),
            PRIMARY KEY (Key)
        );
    )", databaseName.c_str(), tableName.c_str()));
    FillTable(env, databaseName, tableName, rowCount);
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

ui64 GetRowCount(TTestActorRuntime& runtime, ui32 nodeIndex, TPathId pathId) {
    auto statServiceId = NStat::MakeStatServiceID(runtime.GetNodeId(nodeIndex));
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

    return stat.RowCount;
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

    void TestNotFullStatistics(TTestEnv& env, size_t expectedRowCount) {
        auto& runtime = *env.GetServer().GetRuntime();

        auto pathId = ResolvePathId(runtime, "/Root/Database/Table");

        TBlockEvents<TEvDataShard::TEvPeriodicTableStats> block(runtime);
        runtime.WaitFor("TEvPeriodicTableStats", [&]{ return block.size() >= 3; });
        block.Unblock(3);

        bool firstStatsToSA = false;
        auto statsObserver1 = runtime.AddObserver<TEvStatistics::TEvSchemeShardStats>([&](auto&){
            firstStatsToSA = true;
        });
        runtime.WaitFor("TEvSchemeShardStats 1", [&]{ return firstStatsToSA; });

        UNIT_ASSERT(GetRowCount(runtime, 1, pathId) == 0);

        block.Unblock();
        block.Stop();

        bool secondStatsToSA = false;
        auto statsObserver2 = runtime.AddObserver<TEvStatistics::TEvSchemeShardStats>([&](auto&){
            secondStatsToSA = true;
        });
        runtime.WaitFor("TEvSchemeShardStats 2", [&]{ return secondStatsToSA; });

        bool propagate = false;
        auto propagateObserver = runtime.AddObserver<TEvStatistics::TEvPropagateStatistics>([&](auto&){
            propagate = true;
        });
        runtime.WaitFor("TEvPropagateStatistics", [&]{ return propagate; });

        UNIT_ASSERT(GetRowCount(runtime, 1, pathId) == expectedRowCount);
    }

    Y_UNIT_TEST(NotFullStatisticsDatashard) {
        TTestEnv env(1, 1);

        CreateDatabase(env, "Database");
        CreateUniformTable(env, "Database", "Table");

        TestNotFullStatistics(env, 4);
    }

    Y_UNIT_TEST(NotFullStatisticsColumnshard) {
        TTestEnv env(1, 1);

        CreateDatabase(env, "Database");
        CreateColumnStoreTable(env, "Database", "Table", 4);

        TestNotFullStatistics(env, 1000);
    }

    Y_UNIT_TEST(SimpleGlobalIndex) {
        TTestEnv env(1, 1);

        CreateDatabase(env, "Database");
        CreateTableWithGlobalIndex(env, "Database", "Table", 5);

        auto& runtime = *env.GetServer().GetRuntime();
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table/ValueIndex/indexImplTable");
        ValidateRowCount(runtime, 1, pathId, 5);
    }

    Y_UNIT_TEST(ServerlessGlobalIndex) {
        TTestEnv env(1, 1);

        CreateDatabase(env, "Shared", 1, true);
        CreateServerlessDatabase(env, "Serverless", "/Root/Shared");
        CreateTableWithGlobalIndex(env, "Serverless", "Table", 5);

        auto& runtime = *env.GetServer().GetRuntime();
        auto pathId = ResolvePathId(runtime, "/Root/Serverless/Table/ValueIndex/indexImplTable");
        ValidateRowCount(runtime, 1, pathId, 5);
    }
}

} // NSysView
} // NKikimr
