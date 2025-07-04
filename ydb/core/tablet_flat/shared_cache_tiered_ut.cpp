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
            UNIT_ASSERT_VALUES_EQUAL(p.CacheTier, ECacheTier::None);
            result.push_back(p.Id);
        }
        return result;
    }

    TVector<ui32> MoveTouch(auto& cache, TPage& page, ECacheTier tier) {
        auto evicted = cache.MoveTouch(&page, tier);
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheTier, ECacheTier::None);
            result.push_back(p.Id);
        }
        return result;
    }

    void Erase(auto& cache, TPage& page) {
        cache.Erase(&page);
        UNIT_ASSERT_VALUES_EQUAL(page.CacheTier, ECacheTier::None);
    }

    TVector<ui32> TryMove(auto& cache, TPage& page, ECacheTier tier) {
        auto evicted = cache.TryMove(&page, tier);
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheTier, ECacheTier::None);
            result.push_back(p.Id);
        }
        return result;
    }

    TVector<ui32> EvictNext(auto& cache) {
        auto evicted = cache.EvictNext();
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheTier, ECacheTier::None);
            result.push_back(p.Id);
        }
        return result;
    }

    TVector<ui32> Switch(auto& cache, auto&& cache2, auto& counter) {
        auto evicted = cache.Switch(std::move(cache2), counter);
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheTier, ECacheTier::None);
            result.push_back(p.Id);
        }
        return result;
    }

    struct TExpectedCountersValues {
        ui64 RegularBytes;
        ui64 TryKeepInMemoryBytes;
        ui64 RegularLimit;
        ui64 TryKeepInMemoryLimit;
    };

    void CheckCounters(TSharedPageCacheCounters& counters, const TExpectedCountersValues& expected) {
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(ECacheTier::Regular)->Val(), expected.RegularBytes);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(ECacheTier::TryKeepInMemory)->Val(), expected.TryKeepInMemoryBytes);
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
        CheckCounters(counters, {.RegularBytes = 3, .TryKeepInMemoryBytes = 0, .RegularLimit = 5, .TryKeepInMemoryLimit = 5});

        TPage page2{2, 5};
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page2, ECacheTier::TryKeepInMemory), (TVector<ui32>{}));
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 3b}; TryKeepInMemoryTier: {2 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckCounters(counters, {.RegularBytes = 3, .TryKeepInMemoryBytes = 5, .RegularLimit = 5, .TryKeepInMemoryLimit = 5});

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 3b}; TryKeepInMemoryTier: {2 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckCounters(counters, {.RegularBytes = 3, .TryKeepInMemoryBytes = 5, .RegularLimit = 5, .TryKeepInMemoryLimit = 5});

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 3b}; TryKeepInMemoryTier: {2 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckCounters(counters, {.RegularBytes = 3, .TryKeepInMemoryBytes = 5, .RegularLimit = 5, .TryKeepInMemoryLimit = 5});

        TPage page3{3, 2};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), (TVector<ui32>{}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 3b}, {3 2b}; TryKeepInMemoryTier: {2 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularBytes = 5, .TryKeepInMemoryBytes = 5, .RegularLimit = 5, .TryKeepInMemoryLimit = 5});

        TPage page4{4, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), (TVector<ui32>{1}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {3 2b}, {4 3b}; TryKeepInMemoryTier: {2 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularBytes = 5, .TryKeepInMemoryBytes = 5, .RegularLimit = 5, .TryKeepInMemoryLimit = 5});

        cache.UpdateLimit(8, 4);

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), (TVector<ui32>{3}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {4 3b}; TryKeepInMemoryTier: {2 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckCounters(counters, {.RegularBytes = 3, .TryKeepInMemoryBytes = 5, .RegularLimit = 4, .TryKeepInMemoryLimit = 4});

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), (TVector<ui32>{2}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {4 3b}; TryKeepInMemoryTier: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 3);
        CheckCounters(counters, {.RegularBytes = 3, .TryKeepInMemoryBytes = 0, .RegularLimit = 4, .TryKeepInMemoryLimit = 4});
    }

    Y_UNIT_TEST(MoveTouch) {
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TTieredCache<TPage, TPageTraits> cache(10, MakeHolder<TSimpleCache>, NKikimrSharedCache::S3FIFO, counters);
        cache.UpdateLimit(10, 3);

        TPage page1{1, 2};
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page1, ECacheTier::Regular), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 2b}; TryKeepInMemoryTier: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 2);
        CheckCounters(counters, {.RegularBytes = 2, .TryKeepInMemoryBytes = 0, .RegularLimit = 7, .TryKeepInMemoryLimit = 3});

        TPage page2{2, 3};
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page2, ECacheTier::TryKeepInMemory), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 2b}; TryKeepInMemoryTier: {2 3b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 5);
        CheckCounters(counters, {.RegularBytes = 2, .TryKeepInMemoryBytes = 3, .RegularLimit = 7, .TryKeepInMemoryLimit = 3});

        TPage page3{3, 3};
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page3, ECacheTier::Regular), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 2b}, {3 3b}; TryKeepInMemoryTier: {2 3b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckCounters(counters, {.RegularBytes = 5, .TryKeepInMemoryBytes = 3, .RegularLimit = 7, .TryKeepInMemoryLimit = 3});

        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page4, ECacheTier::Regular), (TVector<ui32>{1}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {3 3b}, {4 4b}; TryKeepInMemoryTier: {2 3b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularBytes = 7, .TryKeepInMemoryBytes = 3, .RegularLimit = 7, .TryKeepInMemoryLimit = 3});

        TPage page5{5, 2};
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page5, ECacheTier::Regular), (TVector<ui32>{3}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {4 4b}, {5 2b}; TryKeepInMemoryTier: {2 3b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 9);
        CheckCounters(counters, {.RegularBytes = 6, .TryKeepInMemoryBytes = 3, .RegularLimit = 7, .TryKeepInMemoryLimit = 3});

        TPage page6{6, 3};
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page6, ECacheTier::TryKeepInMemory), (TVector<ui32>{2}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {4 4b}, {5 2b}; TryKeepInMemoryTier: {6 3b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 9);
        CheckCounters(counters, {.RegularBytes = 6, .TryKeepInMemoryBytes = 3, .RegularLimit = 7, .TryKeepInMemoryLimit = 3});

        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page6, ECacheTier::Regular), (TVector<ui32>{4}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {5 2b}, {6 3b}; TryKeepInMemoryTier: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 5);
        CheckCounters(counters, {.RegularBytes = 5, .TryKeepInMemoryBytes = 0, .RegularLimit = 7, .TryKeepInMemoryLimit = 3});
    }

    Y_UNIT_TEST(TryMove) {
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TTieredCache<TPage, TPageTraits> cache(20, MakeHolder<TSimpleCache>, NKikimrSharedCache::S3FIFO, counters);
        cache.UpdateLimit(20, 10);

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page1, ECacheTier::Regular), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page2, ECacheTier::TryKeepInMemory), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page3, ECacheTier::Regular), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page4, ECacheTier::TryKeepInMemory), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}, {3 3b}; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularBytes = 4, .TryKeepInMemoryBytes = 6, .RegularLimit = 10, .TryKeepInMemoryLimit = 10});

        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page2, ECacheTier::TryKeepInMemory), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}, {3 3b}; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularBytes = 4, .TryKeepInMemoryBytes = 6, .RegularLimit = 10, .TryKeepInMemoryLimit = 10});

        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page3, ECacheTier::TryKeepInMemory), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}; TryKeepInMemoryTier: {2 2b}, {4 4b}, {3 3b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularBytes = 1, .TryKeepInMemoryBytes = 9, .RegularLimit = 10, .TryKeepInMemoryLimit = 10});

        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page1, ECacheTier::TryKeepInMemory), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: ; TryKeepInMemoryTier: {2 2b}, {4 4b}, {3 3b}, {1 1b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularBytes = 0, .TryKeepInMemoryBytes = 10, .RegularLimit = 10, .TryKeepInMemoryLimit = 10});

        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page4, ECacheTier::Regular), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {4 4b}; TryKeepInMemoryTier: {2 2b}, {3 3b}, {1 1b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularBytes = 4, .TryKeepInMemoryBytes = 6, .RegularLimit = 10, .TryKeepInMemoryLimit = 10});

        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page1, ECacheTier::Regular), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page2, ECacheTier::Regular), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page3, ECacheTier::Regular), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {4 4b}, {1 1b}, {2 2b}, {3 3b}; TryKeepInMemoryTier: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularBytes = 10, .TryKeepInMemoryBytes = 0, .RegularLimit = 10, .TryKeepInMemoryLimit = 10});

        TPage page5{5, 5};
        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page5, ECacheTier::TryKeepInMemory), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {4 4b}, {1 1b}, {2 2b}, {3 3b}; TryKeepInMemoryTier: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularBytes = 10, .TryKeepInMemoryBytes = 0, .RegularLimit = 10, .TryKeepInMemoryLimit = 10});

        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page5, ECacheTier::TryKeepInMemory), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {4 4b}, {1 1b}, {2 2b}, {3 3b}; TryKeepInMemoryTier: {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 15);
        CheckCounters(counters, {.RegularBytes = 10, .TryKeepInMemoryBytes = 5, .RegularLimit = 10, .TryKeepInMemoryLimit = 10});

        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page5, ECacheTier::Regular), (TVector<ui32>{4, 1}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {2 2b}, {3 3b}, {5 5b}; TryKeepInMemoryTier: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularBytes = 10, .TryKeepInMemoryBytes = 0, .RegularLimit = 10, .TryKeepInMemoryLimit = 10});
    }

    Y_UNIT_TEST(Erase) {
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TTieredCache<TPage, TPageTraits> cache(10, MakeHolder<TSimpleCache>, NKikimrSharedCache::S3FIFO, counters);
        cache.UpdateLimit(10, 6);

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page1, ECacheTier::Regular), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page2, ECacheTier::TryKeepInMemory), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page4, ECacheTier::TryKeepInMemory), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page3, ECacheTier::Regular), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}, {3 3b}; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularBytes = 4, .TryKeepInMemoryBytes = 6, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});
        
        Erase(cache, page2);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}, {3 3b}; TryKeepInMemoryTier: {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckCounters(counters, {.RegularBytes = 4, .TryKeepInMemoryBytes = 4, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        TPage page5{5, 4};
        Erase(cache, page5);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}, {3 3b}; TryKeepInMemoryTier: {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckCounters(counters, {.RegularBytes = 4, .TryKeepInMemoryBytes = 4, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        Erase(cache, page3);

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}; TryKeepInMemoryTier: {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 5);
        CheckCounters(counters, {.RegularBytes = 1, .TryKeepInMemoryBytes = 4, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});
    }

    Y_UNIT_TEST(EvictNext) {
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TTieredCache<TPage, TPageTraits> cache(10, MakeHolder<TSimpleCache>, NKikimrSharedCache::S3FIFO, counters);
        cache.UpdateLimit(10, 6);

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page1, ECacheTier::Regular), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page2, ECacheTier::TryKeepInMemory), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page3, ECacheTier::Regular), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page4, ECacheTier::TryKeepInMemory), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}, {3 3b}; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularBytes = 4, .TryKeepInMemoryBytes = 6, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{1});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {3 3b}; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 9);
        CheckCounters(counters, {.RegularBytes = 3, .TryKeepInMemoryBytes = 6, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{3});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: ; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 6);
        CheckCounters(counters, {.RegularBytes = 0, .TryKeepInMemoryBytes = 6, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{2});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: ; TryKeepInMemoryTier: {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 4);
        CheckCounters(counters, {.RegularBytes = 0, .TryKeepInMemoryBytes = 4, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{4});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: ; TryKeepInMemoryTier: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 0);
        CheckCounters(counters, {.RegularBytes = 0, .TryKeepInMemoryBytes = 0, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});
    }

    Y_UNIT_TEST(UpdateLimit) {
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TTieredCache<TPage, TPageTraits> cache(10, MakeHolder<TSimpleCache>, NKikimrSharedCache::S3FIFO, counters);

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: ; TryKeepInMemoryTier: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 0);
        CheckCounters(counters, {.RegularBytes = 00, .TryKeepInMemoryBytes = 0, .RegularLimit = 10, .TryKeepInMemoryLimit = 0});

        cache.UpdateLimit(10, 6);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: ; TryKeepInMemoryTier: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 0);
        CheckCounters(counters, {.RegularBytes = 0, .TryKeepInMemoryBytes = 0, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page3, ECacheTier::Regular), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page1, ECacheTier::Regular), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page2, ECacheTier::TryKeepInMemory), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page4, ECacheTier::TryKeepInMemory), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {3 3b}, {1 1b}; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularBytes = 4, .TryKeepInMemoryBytes = 6, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        cache.UpdateLimit(15, 6);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {3 3b}, {1 1b}; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularBytes = 4, .TryKeepInMemoryBytes = 6, .RegularLimit = 9, .TryKeepInMemoryLimit = 6});

        TPage page5{5, 5};
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page5, ECacheTier::Regular), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {3 3b}, {1 1b}, {5 5b}; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 15);
        CheckCounters(counters, {.RegularBytes = 9, .TryKeepInMemoryBytes = 6, .RegularLimit = 9, .TryKeepInMemoryLimit = 6});

        cache.UpdateLimit(15, 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {3 3b}, {1 1b}, {5 5b}; TryKeepInMemoryTier: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 15);
        CheckCounters(counters, {.RegularBytes = 9, .TryKeepInMemoryBytes = 6, .RegularLimit = 4, .TryKeepInMemoryLimit = 11});

        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page5, ECacheTier::TryKeepInMemory), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {3 3b}, {1 1b}; TryKeepInMemoryTier: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 15);
        CheckCounters(counters, {.RegularBytes = 4, .TryKeepInMemoryBytes = 11, .RegularLimit = 4, .TryKeepInMemoryLimit = 11});

        cache.UpdateLimit(13, 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {3 3b}, {1 1b}; TryKeepInMemoryTier: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 15);
        CheckCounters(counters, {.RegularBytes = 4, .TryKeepInMemoryBytes = 11, .RegularLimit = 2, .TryKeepInMemoryLimit = 11});

        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page1, ECacheTier::Regular), TVector<ui32>{3});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}; TryKeepInMemoryTier: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 12);
        CheckCounters(counters, {.RegularBytes = 1, .TryKeepInMemoryBytes = 11, .RegularLimit = 2, .TryKeepInMemoryLimit = 11});

        cache.UpdateLimit(6, 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}; TryKeepInMemoryTier: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 12);
        CheckCounters(counters, {.RegularBytes = 1, .TryKeepInMemoryBytes = 11, .RegularLimit = 0, .TryKeepInMemoryLimit = 6});

        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page1, ECacheTier::Regular), (TVector<ui32>{1}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: ; TryKeepInMemoryTier: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 11);
        CheckCounters(counters, {.RegularBytes = 0, .TryKeepInMemoryBytes = 11, .RegularLimit = 0, .TryKeepInMemoryLimit = 6});

        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page5, ECacheTier::TryKeepInMemory), (TVector<ui32>{2, 4}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: ; TryKeepInMemoryTier: {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 5);
        CheckCounters(counters, {.RegularBytes = 0, .TryKeepInMemoryBytes = 5, .RegularLimit = 0, .TryKeepInMemoryLimit = 6});
    }

    Y_UNIT_TEST(Switch) {
        TCounterPtr sizeCounter2 = MakeIntrusive<NMonitoring::TCounterForPtr>();
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());
        TTieredCache<TPage, TPageTraits> cache(10, MakeHolder<TSimpleCache>, NKikimrSharedCache::S3FIFO, counters);
        cache.UpdateLimit(10, 2);

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page1, ECacheTier::Regular), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page2, ECacheTier::TryKeepInMemory), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page3, ECacheTier::Regular), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}, {3 3b}; TryKeepInMemoryTier: {2 2b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 6);
        CheckCounters(counters, {.RegularBytes = 4, .TryKeepInMemoryBytes = 2, .RegularLimit = 8, .TryKeepInMemoryLimit = 2});
        UNIT_ASSERT_VALUES_EQUAL(counters.ReplacementPolicySize(NKikimrSharedCache::S3FIFO)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(sizeCounter2->Val(), 0);

        UNIT_ASSERT_VALUES_EQUAL(Switch(cache, MakeHolder<TSimpleCache>, sizeCounter2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}, {3 3b}; ; TryKeepInMemoryTier: {2 2b}; ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 6);
        CheckCounters(counters, {.RegularBytes = 4, .TryKeepInMemoryBytes = 2, .RegularLimit = 8, .TryKeepInMemoryLimit = 2});
        UNIT_ASSERT_VALUES_EQUAL(counters.ReplacementPolicySize(NKikimrSharedCache::S3FIFO)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(sizeCounter2->Val(), 0);

        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page1, ECacheTier::Regular), TVector<ui32>{});
        cache.UpdateLimit(10, 6);
        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page4, ECacheTier::TryKeepInMemory), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: {1 1b}, {3 3b}; TryKeepInMemoryTier: {4 4b}, {2 2b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckCounters(counters, {.RegularBytes = 4, .TryKeepInMemoryBytes = 6, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});
        UNIT_ASSERT_VALUES_EQUAL(counters.ReplacementPolicySize(NKikimrSharedCache::S3FIFO)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sizeCounter2->Val(), 10);
    }
}

} // namespace NKikimr::NSharedCache
