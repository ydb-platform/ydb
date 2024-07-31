#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <thread>

namespace NKikimr {
namespace NStat {

namespace {


} // namespace

Y_UNIT_TEST_SUITE(AnalyzeColumnshard) {

    Y_UNIT_TEST(AnalyzeOneColumnTable) {
        TTestEnv env(1, 1);
        auto init = [&] () {
            CreateDatabase(env, "Database");
            CreateColumnStoreTable(env, "Database", "Table", 1);
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(10));
        initThread.join();

        auto sender = runtime.AllocateEdgeActor();
        ui64 columnShardId = GetColumnTableShards(runtime, sender, "/Root/Database/Table").at(0);

        ui64 saTabletId = 0;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        AnalyzeTable(runtime, pathId, columnShardId);

        Analyze(runtime, {pathId}, saTabletId);
    }

    Y_UNIT_TEST(AnalyzeTwoColumnTables) {
        TTestEnv env(1, 1);
        auto init = [&] () {
            CreateDatabase(env, "Database");
            CreateColumnStoreTable(env, "Database", "Table1", 1);
            CreateColumnStoreTable(env, "Database", "Table2", 1);
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(10));
        initThread.join();

        ui64 saTabletId = 0;
        auto pathId1 = ResolvePathId(runtime, "/Root/Database/Table1", nullptr, &saTabletId);
        auto pathId2 = ResolvePathId(runtime, "/Root/Database/Table2");

        Analyze(runtime, {pathId1, pathId2}, saTabletId);
    }
}

} // NStat
} // NKikimr
