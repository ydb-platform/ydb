#include <ydb/core/statistics/ut_common/ut_common.h>

#include <ydb/library/actors/testlib/test_runtime.h>

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>

#include <thread>

namespace NKikimr {
namespace NStat {

Y_UNIT_TEST_SUITE(TraverseColumnShard) {

    Y_UNIT_TEST(TraverseColumnTable) {
        TTestEnv env(1, 1);
        auto init = [&] () {
            CreateDatabase(env, "Database");
            CreateColumnStoreTable(env, "Database", "Table", 10);
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(30));
        initThread.join();

        auto pathId = ResolvePathId(runtime, "/Root/Database/Table");

        runtime.SimulateSleep(TDuration::Seconds(30));

        auto countMin = ExtractCountMin(runtime, pathId);

        ui32 value = 1;
        auto probe = countMin->Probe((const char *)&value, sizeof(value));
        UNIT_ASSERT_VALUES_EQUAL(probe, 10);
    }

    Y_UNIT_TEST(TraverseColumnTableRebootSaTabletBeforeResolve) {
        TTestEnv env(1, 1);
        auto init = [&] () {
            CreateDatabase(env, "Database");
            CreateColumnStoreTable(env, "Database", "Table", 10);
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(5));
        initThread.join();

        ui64 saTabletId = 0;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        auto sender = runtime.AllocateEdgeActor();
        int observerCount = 0;
        auto observer = runtime.AddObserver<TEvTxProxySchemeCache::TEvResolveKeySetResult>(
            [&](TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev)
        {
            if (observerCount++ == 2) {
                RebootTablet(runtime, saTabletId, sender);
            }
        });

        runtime.SimulateSleep(TDuration::Seconds(30));

        auto countMin = ExtractCountMin(runtime, pathId);

        ui32 value = 1;
        auto probe = countMin->Probe((const char *)&value, sizeof(value));
        UNIT_ASSERT_VALUES_EQUAL(probe, 10);
    }

    Y_UNIT_TEST(TraverseColumnTableRebootSaTabletBeforeReqDistribution) {
        TTestEnv env(1, 1);
        auto init = [&] () {
            CreateDatabase(env, "Database");
            CreateColumnStoreTable(env, "Database", "Table", 10);
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(5));
        initThread.join();

        ui64 saTabletId = 0;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        auto sender = runtime.AllocateEdgeActor();
        bool observerFirstExec = true;
        auto observer = runtime.AddObserver<TEvHive::TEvRequestTabletDistribution>(
            [&](TEvHive::TEvRequestTabletDistribution::TPtr& /*ev*/)
        {
            if (observerFirstExec) {
                observerFirstExec = false;
                RebootTablet(runtime, saTabletId, sender);
            }
        });

        runtime.SimulateSleep(TDuration::Seconds(30));

        auto countMin = ExtractCountMin(runtime, pathId);

        ui32 value = 1;
        auto probe = countMin->Probe((const char *)&value, sizeof(value));
        UNIT_ASSERT_VALUES_EQUAL(probe, 10);
    }

    Y_UNIT_TEST(TraverseColumnTableRebootSaTabletBeforeAggregate) {
        TTestEnv env(1, 1);
        auto init = [&] () {
            CreateDatabase(env, "Database");
            CreateColumnStoreTable(env, "Database", "Table", 10);
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(5));
        initThread.join();

        ui64 saTabletId = 0;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        auto sender = runtime.AllocateEdgeActor();
        bool observerFirstExec = true;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAggregateStatistics>(
            [&](TEvStatistics::TEvAggregateStatistics::TPtr& ev)
        {
            if (observerFirstExec) {
                observerFirstExec = false;
                RebootTablet(runtime, saTabletId, sender);
            }
        });

        runtime.SimulateSleep(TDuration::Seconds(30));

        auto countMin = ExtractCountMin(runtime, pathId);

        ui32 value = 1;
        auto probe = countMin->Probe((const char *)&value, sizeof(value));
        UNIT_ASSERT_VALUES_EQUAL(probe, 10);
    }

    Y_UNIT_TEST(TraverseColumnTableRebootSaTabletBeforeSave) {
        TTestEnv env(1, 1);
        auto init = [&] () {
            CreateDatabase(env, "Database");
            CreateColumnStoreTable(env, "Database", "Table", 10);
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(5));
        initThread.join();

        ui64 saTabletId = 0;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        auto sender = runtime.AllocateEdgeActor();
        bool observerFirstExec = true;
        auto observer = runtime.AddObserver<TEvStatistics::TEvAggregateStatisticsResponse>(
            [&](TEvStatistics::TEvAggregateStatisticsResponse::TPtr& ev)
        {
            if (observerFirstExec) {
                observerFirstExec = false;
                RebootTablet(runtime, saTabletId, sender);
            }
        });

        runtime.SimulateSleep(TDuration::Seconds(30));

        auto countMin = ExtractCountMin(runtime, pathId);

        ui32 value = 1;
        auto probe = countMin->Probe((const char *)&value, sizeof(value));
        UNIT_ASSERT_VALUES_EQUAL(probe, 10);
    }

    Y_UNIT_TEST(TraverseColumnTableRebootSaTabletInAggregate) {
        TTestEnv env(1, 1);
        auto init = [&] () {
            CreateDatabase(env, "Database");
            CreateColumnStoreTable(env, "Database", "Table", 10);
        };
        std::thread initThread(init);

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SimulateSleep(TDuration::Seconds(5));
        initThread.join();

        ui64 saTabletId = 0;
        auto pathId = ResolvePathId(runtime, "/Root/Database/Table", nullptr, &saTabletId);

        auto sender = runtime.AllocateEdgeActor();
        int observerCount = 0;
        auto observer = runtime.AddObserver<TEvStatistics::TEvStatisticsRequest>(
            [&](TEvStatistics::TEvStatisticsRequest::TPtr& ev)
        {
            if (observerCount++ == 5) {
                RebootTablet(runtime, saTabletId, sender);
            }
        });

        runtime.SimulateSleep(TDuration::Seconds(30));

        auto countMin = ExtractCountMin(runtime, pathId);

        ui32 value = 1;
        auto probe = countMin->Probe((const char *)&value, sizeof(value));
        UNIT_ASSERT_VALUES_EQUAL(probe, 10);
    }
}

} // NStat
} // NKikimr
