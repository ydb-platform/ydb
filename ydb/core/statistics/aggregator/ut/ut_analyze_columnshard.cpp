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
        runtime.SimulateSleep(TDuration::Seconds(30));
        initThread.join();

        auto sender = runtime.AllocateEdgeActor();
        ui64 columnShardId = GetColumnTableShards(runtime, sender, "/Root/Database/Table").at(0);

        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr);

        AnalyzeTable(runtime, pathId, columnShardId);
    }
}

} // NStat
} // NKikimr
