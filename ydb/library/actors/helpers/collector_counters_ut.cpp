#include "collector_counters.h"

#include <ydb/library/actors/core/async_frame_cache.h>
#include <ydb/library/actors/core/harmonizer/harmonizer_stats.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NActors {

Y_UNIT_TEST_SUITE(TActorSystemCountersTest) {
    Y_UNIT_TEST(FrameCacheGauges) {
        auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto utils = root->GetSubgroup("counters", "utils");
        TActorSystemCounters counters;
        counters.Init(utils.Get());
        auto group = utils->FindSubgroup("subsystem", "async_frame_cache");
        UNIT_ASSERT(group);
        auto frames = group->FindCounter("CachedFrames");
        auto bytes = group->FindCounter("CachedBytes");
        UNIT_ASSERT(frames && bytes);

        counters.Set(THarmonizerStats{}, TAsyncFrameCache::TProcessStats{});
        UNIT_ASSERT_VALUES_EQUAL(frames->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(bytes->Val(), 0);

        // The collector sums the caches of the actor system's worker threads.
        TAsyncFrameCache first;
        TAsyncFrameCache second;
        first.Release(first.Allocate(1), 1);
        second.Release(second.Allocate(1025), 1025);
        TAsyncFrameCache::TProcessStats stats;
        stats.Add(first.GetCachedStats());
        stats.Add(second.GetCachedStats());
        counters.Set(THarmonizerStats{}, stats);
        UNIT_ASSERT_VALUES_EQUAL(frames->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(bytes->Val(), 3072);

        auto* live = first.Allocate(1024);
        stats = TAsyncFrameCache::TProcessStats{};
        stats.Add(first.GetCachedStats());
        stats.Add(second.GetCachedStats());
        counters.Set(THarmonizerStats{}, stats);
        UNIT_ASSERT_VALUES_EQUAL(frames->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(bytes->Val(), 2048);
        first.Release(live, 1024);
    }
}

} // namespace NActors
