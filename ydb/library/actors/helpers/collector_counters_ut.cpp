#include "collector_counters.h"

#include <ydb/library/actors/core/mon_stats.h>
#include <ydb/library/actors/util/datetime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NActors {

Y_UNIT_TEST_SUITE(ExecutorPoolCounters) {
    Y_UNIT_TEST(PriorityQueueOldestAge) {
        auto group = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TExecutorPoolCounters counters;
        counters.Init(group.Get(), "Batch", 2);
        TExecutorPoolStats poolStats;
        TExecutorThreadStats stats;
        counters.Set(poolStats, stats);
        UNIT_ASSERT(!counters.NormalActivationQueueOldestAgeUs);
        UNIT_ASSERT(!counters.HighActivationQueueOldestAgeUs);

        poolStats.HasPriorityActivationQueues = true;
        counters.Set(poolStats, stats);
        auto poolGroup = group->FindSubgroup("execpool", "Batch");
        auto normal = poolGroup->FindCounter("NormalActivationQueueOldestAgeUs");
        auto high = poolGroup->FindCounter("HighActivationQueueOldestAgeUs");
        UNIT_ASSERT(normal && high);
        UNIT_ASSERT_VALUES_EQUAL(normal->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(high->Val(), 0);

        const ui64 now = GetCycleCountFast();
        poolStats.OldestNormalActivationTs = now - Us2Ts(2'000'000);
        poolStats.OldestHighActivationTs = now - Us2Ts(1'000'000);
        counters.Set(poolStats, stats);
        UNIT_ASSERT(normal->Val() >= 1'999'999);
        UNIT_ASSERT(high->Val() >= 999'999);
        UNIT_ASSERT(normal->Val() > high->Val());
        const auto previousNormal = normal->Val();
        const auto previousHigh = high->Val();
        counters.Set(poolStats, stats);
        UNIT_ASSERT(normal->Val() >= previousNormal);
        UNIT_ASSERT(high->Val() >= previousHigh);

        // Empty queues reset to zero; they do not keep aging after a Pop.
        poolStats.OldestHighActivationTs = 0;
        counters.Set(poolStats, stats);
        UNIT_ASSERT_VALUES_EQUAL(high->Val(), 0);
        UNIT_ASSERT(normal->Val() >= previousNormal);
        poolStats.OldestNormalActivationTs = 0;
        counters.Set(poolStats, stats);
        UNIT_ASSERT_VALUES_EQUAL(normal->Val(), 0);

        // A clock skew must not underflow into an enormous age.
        poolStats.OldestHighActivationTs = Max<ui64>();
        counters.Set(poolStats, stats);
        UNIT_ASSERT_VALUES_EQUAL(high->Val(), 0);
    }
}

} // namespace NActors
