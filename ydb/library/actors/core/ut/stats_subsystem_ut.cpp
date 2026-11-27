#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/subsystems/stats.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(TActorSystemStatsTest) {
    Y_UNIT_TEST(DefaultSubsystemReturnsEachPoolAndResizesSnapshots) {
        auto setup = MakeHolder<TActorSystemSetup>();
        setup->NodeId = 1;
        setup->ExecutorsCount = 2;
        setup->Executors.Reset(new TAutoPtr<IExecutorPool>[2]);
        setup->Executors[0] = new TBasicExecutorPool(0, 1, 10, "first");
        setup->Executors[1] = new TBasicExecutorPool(1, 2, 10, "second");
        setup->Scheduler = new TBasicSchedulerThread;
        TActorSystem system(setup);
        system.Start();
        const auto& stats = GetActorSystemStats(system);
        UNIT_ASSERT_VALUES_EQUAL(&stats, system.GetSubSystem<TActorSystemStatsSubSystem>());

        TVector<TExecutorThreadStats> threads(10);
        TVector<TExecutorThreadStats> shared;
        std::vector<TExecutorPoolState> states(10);
        stats.GetExecutorPoolStates(states);
        UNIT_ASSERT_VALUES_EQUAL(states.size(), 2);
        for (ui32 pool = 0; pool < 2; ++pool) {
            TExecutorPoolStats poolStats;
            stats.GetPoolStats(pool, poolStats, threads);
            UNIT_ASSERT_VALUES_EQUAL(poolStats.CurrentThreadCount, pool + 1);
            UNIT_ASSERT_VALUES_EQUAL(poolStats.DefaultThreadCount, pool + 1);
            UNIT_ASSERT_VALUES_EQUAL(poolStats.MaxThreadCount, pool + 1);
            UNIT_ASSERT_VALUES_EQUAL(threads.size(), pool + 2); // Pool counters followed by per-thread counters.
            threads.resize(10);
            stats.GetPoolStats(pool, poolStats, threads, shared);
            UNIT_ASSERT_VALUES_EQUAL(poolStats.CurrentThreadCount, pool + 1);
            UNIT_ASSERT_VALUES_EQUAL(threads.size(), pool + 2);
            UNIT_ASSERT(shared.empty());

            TExecutorPoolState state;
            stats.GetExecutorPoolState(pool, state);
            UNIT_ASSERT_VALUES_EQUAL(state.CurrentLimit, pool + 1);
            UNIT_ASSERT_VALUES_EQUAL(state.MaxLimit, pool + 1);
            UNIT_ASSERT_VALUES_EQUAL(state.PossibleMaxLimit, pool + 1);
            UNIT_ASSERT_VALUES_EQUAL(states[pool].CurrentLimit, state.CurrentLimit);
            UNIT_ASSERT_VALUES_EQUAL(states[pool].MinLimit, state.MinLimit);
            UNIT_ASSERT_VALUES_EQUAL(states[pool].MaxLimit, state.MaxLimit);
        }

        THarmonizerStats harmonizer;
        harmonizer.Budget = 123;
        harmonizer.SharedFreeCpu = 456;
        stats.GetHarmonizerStats(harmonizer);
        UNIT_ASSERT_VALUES_EQUAL(harmonizer.Budget, 0);
        UNIT_ASSERT_VALUES_EQUAL(harmonizer.SharedFreeCpu, 0);
        system.Stop();
    }

    Y_UNIT_TEST(ActorContextAccessorReturnsSameRegisteredSubsystem) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        const auto* expected = &GetActorSystemStats(*runtime.GetActorSystem(0));
        const auto* actual = runtime.RunCall([] { return &GetActorSystemStats(); });
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }
}
