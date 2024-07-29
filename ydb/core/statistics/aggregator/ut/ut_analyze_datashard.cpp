#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>

#include <thread>

namespace NKikimr {
namespace NStat {

namespace {


} // namespace

Y_UNIT_TEST_SUITE(AnalyzeDatashard) {

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

        ui64 saTabletId;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        runtime.SimulateSleep(TDuration::Seconds(30));

        auto ev = std::make_unique<TEvStatistics::TEvAnalyze>();
        auto& record = ev->Record;
        PathIdFromPathId(pathId, record.AddTables()->MutablePathId());

        auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(saTabletId, sender, ev.release());
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);

        ValidateCountMin(runtime, pathId);
    }

    Y_UNIT_TEST(ScanTwoTables) {
        TTestEnv env(1, 1);
        auto init = [&] () {
            CreateDatabase(env, "Database");
            CreateUniformTable(env, "Database", "Table1");
            CreateUniformTable(env, "Database", "Table2");
        };
        // TODO remove thread
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(5));
        initThread.join();

        // TODO remove sleep
        runtime.SimulateSleep(TDuration::Seconds(30));

        ui64 saTabletId1;
        auto pathId1 = ResolvePathId(runtime, "/Root/Database/Table1", nullptr, &saTabletId1);
        auto pathId2 = ResolvePathId(runtime, "/Root/Database/Table2");

        auto ev = std::make_unique<TEvStatistics::TEvAnalyze>();
        auto& record = ev->Record;
        PathIdFromPathId(pathId1, record.AddTables()->MutablePathId());
        PathIdFromPathId(pathId2, record.AddTables()->MutablePathId());

        auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(saTabletId1, sender, ev.release());
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);

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

        ui64 saTabletId = 0;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        auto init2 = [&] () {
            DropTable(env, "Database", "Table");
        };
        std::thread init2Thread(init2);

        runtime.SimulateSleep(TDuration::Seconds(5));
        init2Thread.join();

        auto ev = std::make_unique<TEvStatistics::TEvAnalyze>();
        auto& record = ev->Record;
        PathIdFromPathId(pathId, record.AddTables()->MutablePathId());

        auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(saTabletId, sender, ev.release());

        runtime.SimulateSleep(TDuration::Seconds(60));

        ValidateCountMinAbsense(runtime, pathId);
    }
}

} // NStat
} // NKikimr
