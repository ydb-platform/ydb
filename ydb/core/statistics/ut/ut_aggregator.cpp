#include "ut_common.h"

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/stat_service.h>

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

void DropTable(TTestEnv& env, const TString& databaseName, const TString& tableName) {
    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();

    auto result = session.ExecuteSchemeQuery(Sprintf(R"(
        DROP TABLE `Root/%s/%s`;
    )", databaseName.c_str(), tableName.c_str())).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void ValidateCountMin(TTestActorRuntime& runtime, TPathId pathId) {
    auto statServiceId = NStat::MakeStatServiceID(runtime.GetNodeId(1));

    NStat::TRequest req;
    req.PathId = pathId;
    req.ColumnName = "Key";

    auto evGet = std::make_unique<TEvStatistics::TEvGetStatistics>();
    evGet->StatType = NStat::EStatType::COUNT_MIN_SKETCH;
    evGet->StatRequests.push_back(req);

    auto sender = runtime.AllocateEdgeActor(1);
    runtime.Send(statServiceId, sender, evGet.release(), 1, true);
    auto evResult = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvGetStatisticsResult>(sender);

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

void ValidateCountMinAbsense(TTestActorRuntime& runtime, TPathId pathId) {
    auto statServiceId = NStat::MakeStatServiceID(runtime.GetNodeId(1));

    NStat::TRequest req;
    req.PathId = pathId;
    req.ColumnName = "Key";

    auto evGet = std::make_unique<TEvStatistics::TEvGetStatistics>();
    evGet->StatType = NStat::EStatType::COUNT_MIN_SKETCH;
    evGet->StatRequests.push_back(req);

    auto sender = runtime.AllocateEdgeActor(1);
    runtime.Send(statServiceId, sender, evGet.release(), 1, true);
    auto evResult = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvGetStatisticsResult>(sender);

    UNIT_ASSERT(evResult);
    UNIT_ASSERT(evResult->Get());
    UNIT_ASSERT(evResult->Get()->StatResponses.size() == 1);

    auto rsp = evResult->Get()->StatResponses[0];
    UNIT_ASSERT(!rsp.Success);
}

} // namespace

Y_UNIT_TEST_SUITE(StatisticsAggregator) {

    Y_UNIT_TEST(ScanOneTable) {
        TTestEnv env(1, 1);
        auto init = [&] () {
            CreateDatabase(env, "Database");
            CreateUniformTable(env, "Database", "Table");
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(5));
        initThread.join();

        ui64 tabletId = 0;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &tabletId);

        auto ev = std::make_unique<TEvStatistics::TEvScanTable>();
        auto& record = ev->Record;
        PathIdFromPathId(pathId, record.MutablePathId());

        auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(tabletId, sender, ev.release());
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvScanTableResponse>(sender);

        ValidateCountMin(runtime, pathId);
    }

    Y_UNIT_TEST(ScanTwoTables) {
        TTestEnv env(1, 1);
        auto init = [&] () {
            CreateDatabase(env, "Database");
            CreateUniformTable(env, "Database", "Table1");
            CreateUniformTable(env, "Database", "Table2");
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(5));
        initThread.join();

        runtime.SimulateSleep(TDuration::Seconds(60));

        auto pathId1 = ResolvePathId(runtime, "/Root/Database/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Database/Table2");

        ValidateCountMin(runtime, pathId1);
        ValidateCountMin(runtime, pathId2);
    }

    Y_UNIT_TEST(ScanOneTableServerless) {
        TTestEnv env(1, 1);

        auto init = [&] () {
            CreateDatabase(env, "Shared");
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(5));
        initThread.join();

        TPathId domainKey;
        ResolvePathId(runtime, "/Root/Shared", &domainKey);

        auto init2 = [&] () {
            CreateServerlessDatabase(env, "Serverless", domainKey);
            CreateUniformTable(env, "Serverless", "Table");
        };
        std::thread init2Thread(init2);

        runtime.SimulateSleep(TDuration::Seconds(5));
        init2Thread.join();

        runtime.SimulateSleep(TDuration::Seconds(60));

        auto pathId = ResolvePathId(runtime, "/Root/Serverless/Table");
        ValidateCountMin(runtime, pathId);
    }

    Y_UNIT_TEST(ScanTwoTablesServerless) {
        TTestEnv env(1, 1);

        auto init = [&] () {
            CreateDatabase(env, "Shared");
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(5));
        initThread.join();

        TPathId domainKey;
        ResolvePathId(runtime, "/Root/Shared", &domainKey);

        auto init2 = [&] () {
            CreateServerlessDatabase(env, "Serverless", domainKey);
            CreateUniformTable(env, "Serverless", "Table1");
            CreateUniformTable(env, "Serverless", "Table2");
        };
        std::thread init2Thread(init2);

        runtime.SimulateSleep(TDuration::Seconds(5));
        init2Thread.join();

        runtime.SimulateSleep(TDuration::Seconds(60));

        auto pathId1 = ResolvePathId(runtime, "/Root/Serverless/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Serverless/Table2");
        ValidateCountMin(runtime, pathId1);
        ValidateCountMin(runtime, pathId2);
    }

    Y_UNIT_TEST(ScanTwoTablesTwoServerlessDbs) {
        TTestEnv env(1, 1);

        auto init = [&] () {
            CreateDatabase(env, "Shared");
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(5));
        initThread.join();

        TPathId domainKey;
        ResolvePathId(runtime, "/Root/Shared", &domainKey);

        auto init2 = [&] () {
            CreateServerlessDatabase(env, "Serverless1", domainKey);
            CreateServerlessDatabase(env, "Serverless2", domainKey);
            CreateUniformTable(env, "Serverless1", "Table1");
            CreateUniformTable(env, "Serverless2", "Table2");
        };
        std::thread init2Thread(init2);

        runtime.SimulateSleep(TDuration::Seconds(5));
        init2Thread.join();

        runtime.SimulateSleep(TDuration::Seconds(60));

        auto pathId1 = ResolvePathId(runtime, "/Root/Serverless1/Table1");
        auto pathId2 = ResolvePathId(runtime, "/Root/Serverless2/Table2");
        ValidateCountMin(runtime, pathId1);
        ValidateCountMin(runtime, pathId2);
    }

    Y_UNIT_TEST(DropTableNavigateError) {
        TTestEnv env(1, 1);
        auto init = [&] () {
            CreateDatabase(env, "Database");
            CreateUniformTable(env, "Database", "Table");
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(5));
        initThread.join();

        ui64 tabletId = 0;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &tabletId);

        auto init2 = [&] () {
            DropTable(env, "Database", "Table");
        };
        std::thread init2Thread(init2);

        runtime.SimulateSleep(TDuration::Seconds(5));
        init2Thread.join();

        auto ev = std::make_unique<TEvStatistics::TEvScanTable>();
        auto& record = ev->Record;
        PathIdFromPathId(pathId, record.MutablePathId());

        auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(tabletId, sender, ev.release());

        runtime.SimulateSleep(TDuration::Seconds(60));

        ValidateCountMinAbsense(runtime, pathId);
    }

}

} // NStat
} // NKikimr
