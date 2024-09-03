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

    Y_UNIT_TEST(AnalyzeOneTable) {
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

        Analyze(runtime, saTabletId, {{pathId}});

        ValidateCountMinDatashardAbsense(runtime, pathId);
    }

    Y_UNIT_TEST(AnalyzeTwoTables) {
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

        Analyze(runtime, saTabletId1, {pathId1, pathId2});

        ValidateCountMinDatashardAbsense(runtime, pathId1);
        ValidateCountMinDatashardAbsense(runtime, pathId2);
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

        Analyze(runtime, saTabletId, {pathId});

        runtime.SimulateSleep(TDuration::Seconds(10));

        ValidateCountMinDatashardAbsense(runtime, pathId);
    }
}

} // NStat
} // NKikimr
