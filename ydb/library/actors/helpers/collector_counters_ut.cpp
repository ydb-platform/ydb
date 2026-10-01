#include "collector_counters.h"
#include <ydb/library/actors/core/subsystems/allocation_cache.h>

#include <ydb/library/actors/core/subsystems/async_frame_cache.h>
#include <ydb/library/actors/core/harmonizer/harmonizer_stats.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NActors {

Y_UNIT_TEST_SUITE(TActorSystemCountersTest) {
    Y_UNIT_TEST(AllAllocationFamiliesAndLegacyGauges) {
        auto root = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TActorSystemCounters counters;
        counters.Init(root.Get());
        counters.SetAllocationCacheStats({{"AsyncFrames", {2, 2048}}, {"Other", {3, 192}}});
        auto group = root->FindSubgroup("subsystem", "allocation_cache");
        UNIT_ASSERT(group);
        auto other = group->FindSubgroup("family", "Other");
        UNIT_ASSERT(other);
        UNIT_ASSERT_VALUES_EQUAL(other->FindCounter("CachedBlocks")->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(other->FindCounter("CachedBytes")->Val(), 192);
        UNIT_ASSERT_VALUES_EQUAL(counters.AsyncFrameCacheCachedFrames->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(counters.AsyncFrameCacheCachedBytes->Val(), 2048);
        counters.SetAllocationCacheStats({{"AsyncFrames", {}}, {"Other", {}}});
        UNIT_ASSERT_VALUES_EQUAL(other->FindCounter("CachedBytes")->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.AsyncFrameCacheCachedFrames->Val(), 0);
    }

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

        counters.SetAllocationCacheStats({});
        UNIT_ASSERT_VALUES_EQUAL(frames->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(bytes->Val(), 0);

        // The collector sums the caches of the actor system's worker threads.
        TAllocationCache<TAsyncFrameCacheTag> first(TAsyncFrameCache::DefaultSizeBytes);
        TAllocationCache<TAsyncFrameCacheTag> second(TAsyncFrameCache::DefaultSizeBytes);
        first.Release(first.Allocate(1), 1);
        second.Release(second.Allocate(1025), 1025);
        TAllocationCacheProcessStats stats;
        stats.Add(first.GetCachedStats());
        stats.Add(second.GetCachedStats());
        counters.SetAllocationCacheStats({{TAsyncFrameCacheTag::Name, stats}});
        UNIT_ASSERT_VALUES_EQUAL(frames->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(bytes->Val(), 3072);

        auto* live = first.Allocate(1024);
        stats = TAllocationCacheProcessStats{};
        stats.Add(first.GetCachedStats());
        stats.Add(second.GetCachedStats());
        counters.SetAllocationCacheStats({{TAsyncFrameCacheTag::Name, stats}});
        UNIT_ASSERT_VALUES_EQUAL(frames->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(bytes->Val(), 2048);
        first.Release(live, 1024);
    }
}

} // namespace NActors
