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

    Y_UNIT_TEST(TraverseColumnTableRebootTabletBeforeResolve) {
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
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvTxProxySchemeCache::EvResolveKeySetResult &&
                observerCount++ == 2)
            {
                RebootTablet(runtime, saTabletId, sender);
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        runtime.SimulateSleep(TDuration::Seconds(30));

        auto countMin = ExtractCountMin(runtime, pathId);

        ui32 value = 1;
        auto probe = countMin->Probe((const char *)&value, sizeof(value));
        UNIT_ASSERT_VALUES_EQUAL(probe, 10);
    }

    Y_UNIT_TEST(TraverseColumnTableRebootTabletBeforeReqDistribution) {
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
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvHive::EvRequestTabletDistribution &&
                observerCount++ == 0)
            {
                RebootTablet(runtime, saTabletId, sender);
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        runtime.SimulateSleep(TDuration::Seconds(30));

        auto countMin = ExtractCountMin(runtime, pathId);

        ui32 value = 1;
        auto probe = countMin->Probe((const char *)&value, sizeof(value));
        UNIT_ASSERT_VALUES_EQUAL(probe, 10);
    }

    Y_UNIT_TEST(TraverseColumnTableRebootTabletBeforeAggregate) {
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
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvStatistics::EvAggregateStatistics &&
                observerCount++ == 0)
            {
                RebootTablet(runtime, saTabletId, sender);
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        runtime.SimulateSleep(TDuration::Seconds(30));

        auto countMin = ExtractCountMin(runtime, pathId);

        ui32 value = 1;
        auto probe = countMin->Probe((const char *)&value, sizeof(value));
        UNIT_ASSERT_VALUES_EQUAL(probe, 10);
    }

    Y_UNIT_TEST(TraverseColumnTableRebootTabletBeforeSave) {
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
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvStatistics::EvAggregateStatisticsResponse &&
                observerCount++ == 0)
            {
                RebootTablet(runtime, saTabletId, sender);
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        runtime.SimulateSleep(TDuration::Seconds(30));

        auto countMin = ExtractCountMin(runtime, pathId);

        ui32 value = 1;
        auto probe = countMin->Probe((const char *)&value, sizeof(value));
        UNIT_ASSERT_VALUES_EQUAL(probe, 10);
    }

    Y_UNIT_TEST(TraverseColumnTableRebootInAggregate) {
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
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvStatistics::EvStatisticsRequest &&
                observerCount++ == 5)
            {
                RebootTablet(runtime, saTabletId, sender);
            }
            return TTestActorRuntime::EEventAction::PROCESS;
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
