#include "executor_pool_basic.h"
#include "executor_thread.h"

#include <library/cpp/testing/unittest/registar.h>

#include <thread>

using namespace NActors;

namespace {
    class TStatsTestThread : public TExecutorThread {
    public:
        using TExecutorThread::TExecutorThread;

        void SetActivity(ui32 activity) {
            ThreadCtx.ActivityContext.ElapsingActorActivity.store(activity);
        }
    };

    void CheckConcurrentPoolSwitch(ui32 activity) {
        TBasicExecutorPool first(0, 1, 0);
        TBasicExecutorPool second(1, 1, 0);
        TStatsTestThread executor(0, nullptr, &first, &first, 2, "stats-test", 0);
        executor.SetActivity(activity);
        std::atomic<bool> started = false;
        std::atomic<bool> done = false;
        std::thread switcher([&] {
            started.store(true);
            while (!done.load()) {
                executor.SwitchPool(&second);
                executor.SwitchPool(&first);
            }
        });
        while (!started.load()) {
            std::this_thread::yield();
        }
        for (ui32 i = 0; i < 10'000; ++i) {
            TExecutorThreadStats stats;
            executor.GetSharedStats(i % 2, stats);
            executor.GetSharedStatsForHarmonizer(i % 2, stats);
            executor.GetCurrentStats(stats);
            executor.GetCurrentStatsForHarmonizer(stats);
        }
        done.store(true);
        switcher.join();

        TExecutorThreadStats firstStats;
        TExecutorThreadStats secondStats;
        executor.GetSharedStats(0, firstStats);
        executor.GetSharedStats(1, secondStats);
        if (activity == SleepActivity) {
            UNIT_ASSERT(firstStats.SafeParkedTicks + secondStats.SafeParkedTicks > 0);
            UNIT_ASSERT_VALUES_EQUAL(firstStats.SafeElapsedTicks + secondStats.SafeElapsedTicks, 0);
        } else {
            UNIT_ASSERT(firstStats.SafeElapsedTicks + secondStats.SafeElapsedTicks > 0);
            UNIT_ASSERT_VALUES_EQUAL(firstStats.SafeParkedTicks + secondStats.SafeParkedTicks, 0);
        }
    }
}

Y_UNIT_TEST_SUITE(ExecutorThreadStats) {
    Y_UNIT_TEST(SwitchPoolWhileCollectingParkedStats) {
        CheckConcurrentPoolSwitch(SleepActivity);
    }

    Y_UNIT_TEST(SwitchPoolWhileCollectingElapsedStats) {
        CheckConcurrentPoolSwitch(TActorTypeOperator::GetActorSystemIndex());
    }
}
