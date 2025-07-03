#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tablet_flat/shared_cache_switchable.h>
#include <ydb/core/tablet_flat/shared_cache_tiered.h>
#include <ydb/core/tablet_flat/ut/shared_cache_ut_common.h>


namespace NKikimr::NSharedCache {

using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
using namespace NKikimr::NSharedCache::NTest;

Y_UNIT_TEST_SUITE(TieredCache) {

    TVector<ui32> Touch(auto& cache, TPage& page) {
        auto evicted = cache.Touch(&page);
        TVector<ui32> result;
        for (auto& p : evicted) {
            result.push_back(p.Id);
        }
        return result;
    }

    TVector<ui32> EvictNext(auto& cache) {
        auto evicted = cache.EvictNext();
        TVector<ui32> result;
        for (auto& p : evicted) {
            result.push_back(p.Id);
        }
        return result;
    }

    TVector<ui32> Switch(auto& cache, auto&& cache2, auto& counter) {
        auto evicted = cache.Switch(std::move(cache2), counter);
        TVector<ui32> result;
        for (auto& p : evicted) {
            result.push_back(p.Id);
        }
        return result;
    }

    struct TExpectedCountersValues {
        ui64 RegularLimit;
        ui64 TryKeepInMemoryLimit;
    };

    void CheckCounters(TSharedPageCacheCounters& counters, const TExpectedCountersValues& expected) {
        UNIT_ASSERT_VALUES_EQUAL(counters.LimitBytesTier(ECacheTier::Regular)->Val(), expected.RegularLimit);
        UNIT_ASSERT_VALUES_EQUAL(counters.LimitBytesTier(ECacheTier::TryKeepInMemory)->Val(), expected.TryKeepInMemoryLimit);
    }

    Y_UNIT_TEST(Touch) {
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TTieredCache<TPage, TPageTraits> cache(10, MakeHolder<TSimpleCache>, NKikimrSharedCache::S3FIFO, counters);
        cache.UpdateLimit(10, 5);

        TPage page1{1, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 3b}; TryKeepInMemoryTier: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 3);
        CheckCounters(counters, {.RegularLimit = 5, .TryKeepInMemoryLimit = 5});

        TPage page2{2, 5};
        page2.CacheTier = ECacheTier::TryKeepInMemory;
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 3b}; TryKeepInMemoryTier: {2 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckCounters(counters, {.RegularLimit = 5, .TryKeepInMemoryLimit = 5});

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 3b}; TryKeepInMemoryTier: {2 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckCounters(counters, {.RegularLimit = 5, .TryKeepInMemoryLimit = 5});

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 3b}; TryKeepInMemoryTier: {2 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckCounters(counters, {.RegularLimit = 5, .TryKeepInMemoryLimit = 5});

        TPage page3{3, 2};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), (TVector<ui32>{}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 3b}, {3 2b}; TryKeepInMemoryTier: {2 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularLimit = 5, .TryKeepInMemoryLimit = 5});

        TPage page4{4, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), (TVector<ui32>{1}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {3 2b}, {4 3b}; TryKeepInMemoryTier: {2 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularLimit = 5, .TryKeepInMemoryLimit = 5});

        cache.UpdateLimit(8, 4);

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), (TVector<ui32>{3}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {4 3b}; TryKeepInMemoryTier: {2 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 4});

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), (TVector<ui32>{2}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {4 3b}; TryKeepInMemoryTier: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 3);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 4});

        cache.Erase(&page4);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: ; TryKeepInMemoryTier: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 0);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 4});

        page4.CacheTier = ECacheTier::TryKeepInMemory;
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), (TVector<ui32>{}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: ; TryKeepInMemoryTier: {4 3b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 3);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 4});
    }

    Y_UNIT_TEST(Erase) {
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TTieredCache<TPage, TPageTraits> cache(10, MakeHolder<TSimpleCache>, NKikimrSharedCache::S3FIFO, counters);
        cache.UpdateLimit(10, 6);

        TPage page1{1, 1};
        TPage page2{2, 2};
        page2.CacheTier = ECacheTier::TryKeepInMemory;
        TPage page3{3, 3};
        TPage page4{4, 4};
        page4.CacheTier = ECacheTier::TryKeepInMemory;
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}, {3 3b}; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 6});
        
        cache.Erase(&page2);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}, {3 3b}; TryKeepInMemoryTier: {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        TPage page5{5, 4};
        cache.Erase(&page5);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}, {3 3b}; TryKeepInMemoryTier: {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        cache.Erase(&page3);

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}; TryKeepInMemoryTier: {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 5);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 6});
    }

    Y_UNIT_TEST(EvictNext) {
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TTieredCache<TPage, TPageTraits> cache(10, MakeHolder<TSimpleCache>, NKikimrSharedCache::S3FIFO, counters);
        cache.UpdateLimit(10, 6);

        TPage page1{1, 1};
        TPage page2{2, 2};
        page2.CacheTier = ECacheTier::TryKeepInMemory;
        TPage page3{3, 3};
        TPage page4{4, 4};
        page4.CacheTier = ECacheTier::TryKeepInMemory;
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}, {3 3b}; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{1});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {3 3b}; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 9);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{3});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: ; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 6);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{2});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: ; TryKeepInMemoryTier: {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 4);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{4});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: ; TryKeepInMemoryTier: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 0);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 6});
    }

    Y_UNIT_TEST(UpdateLimit) {
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TTieredCache<TPage, TPageTraits> cache(10, MakeHolder<TSimpleCache>, NKikimrSharedCache::S3FIFO, counters);

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: ; TryKeepInMemoryTier: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 0);
        CheckCounters(counters, {.RegularLimit = 10, .TryKeepInMemoryLimit = 0});

        cache.UpdateLimit(10, 6);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: ; TryKeepInMemoryTier: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 0);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        TPage page1{1, 1};
        TPage page2{2, 2};
        page2.CacheTier = ECacheTier::TryKeepInMemory;
        TPage page3{3, 3};
        TPage page4{4, 4};
        page4.CacheTier = ECacheTier::TryKeepInMemory;
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {3 3b}, {1 1b}; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        cache.UpdateLimit(15, 6);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {3 3b}, {1 1b}; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularLimit = 9, .TryKeepInMemoryLimit = 6});

        TPage page5{5, 5};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {3 3b}, {1 1b}, {5 5b}; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 15);
        CheckCounters(counters, {.RegularLimit = 9, .TryKeepInMemoryLimit = 6});

        cache.UpdateLimit(15, 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {3 3b}, {1 1b}, {5 5b}; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 15);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 11});

        cache.Erase(&page5);
        page5.CacheTier = ECacheTier::TryKeepInMemory;
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {3 3b}, {1 1b}; TryKeepInMemoryTier: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 15);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 11});

        cache.UpdateLimit(13, 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {3 3b}, {1 1b}; TryKeepInMemoryTier: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 15);
        CheckCounters(counters, {.RegularLimit = 2, .TryKeepInMemoryLimit = 11});

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{3});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}; TryKeepInMemoryTier: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 12);
        CheckCounters(counters, {.RegularLimit = 2, .TryKeepInMemoryLimit = 11});

        cache.UpdateLimit(6, 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}; TryKeepInMemoryTier: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 12);
        CheckCounters(counters, {.RegularLimit = 0, .TryKeepInMemoryLimit = 6});

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), (TVector<ui32>{1}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: ; TryKeepInMemoryTier: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 11);
        CheckCounters(counters, {.RegularLimit = 0, .TryKeepInMemoryLimit = 6});

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), (TVector<ui32>{2, 4}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: ; TryKeepInMemoryTier: {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 5);
        CheckCounters(counters, {.RegularLimit = 0, .TryKeepInMemoryLimit = 6});
    }

    Y_UNIT_TEST(Switch) {
        TCounterPtr sizeCounter2 = MakeIntrusive<NMonitoring::TCounterForPtr>();
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TTieredCache<TPage, TPageTraits> cache(10, MakeHolder<TSimpleCache>, NKikimrSharedCache::S3FIFO, counters);
        cache.UpdateLimit(10, 2);

        TPage page1{1, 1};
        TPage page2{2, 2};
        page2.CacheTier = ECacheTier::TryKeepInMemory;
        TPage page3{3, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}, {3 3b}; TryKeepInMemoryTier: {2 2b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 6);
        CheckCounters(counters, {.RegularLimit = 8, .TryKeepInMemoryLimit = 2});
        UNIT_ASSERT_VALUES_EQUAL(counters.ReplacementPolicySize(NKikimrSharedCache::S3FIFO)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(sizeCounter2->Val(), 0);

        UNIT_ASSERT_VALUES_EQUAL(Switch(cache, MakeHolder<TSimpleCache>, sizeCounter2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}, {3 3b}; ; TryKeepInMemoryTier: {2 2b}; ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 6);
        CheckCounters(counters, {.RegularLimit = 8, .TryKeepInMemoryLimit = 2});
        UNIT_ASSERT_VALUES_EQUAL(counters.ReplacementPolicySize(NKikimrSharedCache::S3FIFO)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(sizeCounter2->Val(), 0);

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        cache.UpdateLimit(10, 6);
        TPage page4{4, 4};
        page4.CacheTier = ECacheTier::TryKeepInMemory;
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}, {3 3b}; TryKeepInMemoryTier: {4 4b}, {2 2b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularLimit = 4, .TryKeepInMemoryLimit = 6});
        UNIT_ASSERT_VALUES_EQUAL(counters.ReplacementPolicySize(NKikimrSharedCache::S3FIFO)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sizeCounter2->Val(), 10);
    }
}

} // namespace NKikimr::NSharedCache
