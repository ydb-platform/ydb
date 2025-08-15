#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tablet_flat/shared_cache_tiered.h>
#include <ydb/core/tablet_flat/ut/shared_cache_ut_common.h>

namespace NKikimr::NSharedCache {

using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
using ECacheMode = NTable::NPage::ECacheMode;
using namespace NKikimr::NSharedCache::NTest;
using TCache = TTieredCache<NTest::TPage, TPageTraits>;
using TStats = TCache::TStats;

Y_UNIT_TEST_SUITE(TieredCache) {

    TVector<ui32> Touch(auto& cache, NTest::TPage& page) {
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

    void CheckStats(TCache& cache, const TStats& expected) {
        auto actual = cache.GetStats();

        UNIT_ASSERT_VALUES_EQUAL(actual.RegularBytes, expected.RegularBytes);
        UNIT_ASSERT_VALUES_EQUAL(actual.TryKeepInMemoryBytes, expected.TryKeepInMemoryBytes);
        UNIT_ASSERT_VALUES_EQUAL(actual.RegularLimit, expected.RegularLimit);
        UNIT_ASSERT_VALUES_EQUAL(actual.TryKeepInMemoryLimit, expected.TryKeepInMemoryLimit);
    }

    Y_UNIT_TEST(Touch) {
        TCache cache(10);
        cache.UpdateLimit(10, 5);

        NTest::TPage page1{1, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue: {1 0f 3b} MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue:  MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 3);
        CheckStats(cache, {.RegularBytes = 3, .TryKeepInMemoryBytes = 0, .RegularLimit = 5, .TryKeepInMemoryLimit = 5});

        NTest::TPage page2{2, 5};
        page2.CacheMode = ECacheMode::TryKeepInMemory;
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue: {1 0f 3b} MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue: {2 0f 5b} MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckStats(cache, {.RegularBytes = 3, .TryKeepInMemoryBytes = 5, .RegularLimit = 5, .TryKeepInMemoryLimit = 5});

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue: {1 1f 3b} MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue: {2 0f 5b} MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckStats(cache, {.RegularBytes = 3, .TryKeepInMemoryBytes = 5, .RegularLimit = 5, .TryKeepInMemoryLimit = 5});

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue: {1 1f 3b} MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue: {2 1f 5b} MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckStats(cache, {.RegularBytes = 3, .TryKeepInMemoryBytes = 5, .RegularLimit = 5, .TryKeepInMemoryLimit = 5});
    }

    Y_UNIT_TEST(Erase) {
        TCache cache(10);
        cache.UpdateLimit(10, 6);

        NTest::TPage page1{1, 1};
        NTest::TPage page2{2, 2};
        page2.CacheMode = ECacheMode::TryKeepInMemory;
        NTest::TPage page3{3, 3};
        NTest::TPage page4{4, 4};
        page4.CacheMode = ECacheMode::TryKeepInMemory;
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue: {1 0f 1b}, {3 0f 3b} MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue: {2 0f 2b}, {4 0f 4b} MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckStats(cache, {.RegularBytes = 4, .TryKeepInMemoryBytes = 6, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});
        
        cache.Erase(&page2);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue: {1 0f 1b}, {3 0f 3b} MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue: {4 0f 4b} MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckStats(cache, {.RegularBytes = 4, .TryKeepInMemoryBytes = 4, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        NTest::TPage page5{5, 4};
        cache.Erase(&page5);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue: {1 0f 1b}, {3 0f 3b} MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue: {4 0f 4b} MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 8);
        CheckStats(cache, {.RegularBytes = 4, .TryKeepInMemoryBytes = 4, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        cache.Erase(&page3);

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue: {1 0f 1b} MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue: {4 0f 4b} MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 5);
        CheckStats(cache, {.RegularBytes = 1, .TryKeepInMemoryBytes = 4, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});
    }

    Y_UNIT_TEST(EvictNext) {
        TCache cache(10);
        cache.UpdateLimit(10, 6);

        NTest::TPage page1{1, 1};
        NTest::TPage page2{2, 2};
        page2.CacheMode = ECacheMode::TryKeepInMemory;
        NTest::TPage page3{3, 3};
        NTest::TPage page4{4, 4};
        page4.CacheMode = ECacheMode::TryKeepInMemory;
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue: {1 0f 1b}, {3 0f 3b} MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue: {2 0f 2b}, {4 0f 4b} MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckStats(cache, {.RegularBytes = 4, .TryKeepInMemoryBytes = 6, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{1});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue: {3 0f 3b} MainQueue:  GhostQueue: 1; TryKeepInMemoryTier: SmallQueue: {2 0f 2b}, {4 0f 4b} MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 9);
        CheckStats(cache, {.RegularBytes = 3, .TryKeepInMemoryBytes = 6, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{3});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue:  MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue: {2 0f 2b}, {4 0f 4b} MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 6);
        CheckStats(cache, {.RegularBytes = 0, .TryKeepInMemoryBytes = 6, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{2});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue:  MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue: {4 0f 4b} MainQueue:  GhostQueue: 2");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 4);
        CheckStats(cache, {.RegularBytes = 0, .TryKeepInMemoryBytes = 4, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{4});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue:  MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue:  MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 0);
        CheckStats(cache, {.RegularBytes = 0, .TryKeepInMemoryBytes = 0, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});
    }

    Y_UNIT_TEST(UpdateLimit) {
        TCache cache(10);

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue:  MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue:  MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 0);
        CheckStats(cache, {.RegularBytes = 0, .TryKeepInMemoryBytes = 0, .RegularLimit = 10, .TryKeepInMemoryLimit = 0});

        cache.UpdateLimit(10, 6);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue:  MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue:  MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 0);
        CheckStats(cache, {.RegularBytes = 0, .TryKeepInMemoryBytes = 0, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        NTest::TPage page1{1, 1};
        NTest::TPage page2{2, 2};
        page2.CacheMode = ECacheMode::TryKeepInMemory;
        NTest::TPage page3{3, 3};
        NTest::TPage page4{4, 4};
        page4.CacheMode = ECacheMode::TryKeepInMemory;
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue: {3 0f 3b}, {1 0f 1b} MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue: {2 0f 2b}, {4 0f 4b} MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckStats(cache, {.RegularBytes = 4, .TryKeepInMemoryBytes = 6, .RegularLimit = 4, .TryKeepInMemoryLimit = 6});

        cache.UpdateLimit(15, 6);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue: {3 0f 3b}, {1 0f 1b} MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue: {2 0f 2b}, {4 0f 4b} MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        CheckStats(cache, {.RegularBytes = 4, .TryKeepInMemoryBytes = 6, .RegularLimit = 9, .TryKeepInMemoryLimit = 6});

        NTest::TPage page5{5, 5};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue: {3 0f 3b}, {1 0f 1b}, {5 0f 5b} MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue: {2 0f 2b}, {4 0f 4b} MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 15);
        CheckStats(cache, {.RegularBytes = 9, .TryKeepInMemoryBytes = 6, .RegularLimit = 9, .TryKeepInMemoryLimit = 6});

        cache.UpdateLimit(15, 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue: {3 0f 3b}, {1 0f 1b}, {5 0f 5b} MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue: {2 0f 2b}, {4 0f 4b} MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 15);
        CheckStats(cache, {.RegularBytes = 9, .TryKeepInMemoryBytes = 6, .RegularLimit = 4, .TryKeepInMemoryLimit = 11});

        cache.Erase(&page5);
        page5.CacheMode = ECacheMode::TryKeepInMemory;
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue: {3 0f 3b}, {1 0f 1b} MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue: {2 0f 2b}, {4 0f 4b}, {5 0f 5b} MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 15);
        CheckStats(cache, {.RegularBytes = 4, .TryKeepInMemoryBytes = 11, .RegularLimit = 4, .TryKeepInMemoryLimit = 11});

        cache.UpdateLimit(13, 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue: {3 0f 3b}, {1 0f 1b} MainQueue:  GhostQueue: ; TryKeepInMemoryTier: SmallQueue: {2 0f 2b}, {4 0f 4b}, {5 0f 5b} MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 15);
        CheckStats(cache, {.RegularBytes = 4, .TryKeepInMemoryBytes = 11, .RegularLimit = 2, .TryKeepInMemoryLimit = 11});

        NTest::TPage page6{6, 1};
        page5.CacheMode = ECacheMode::TryKeepInMemory;
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page6), TVector<ui32>{3});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "RegularTier: SmallQueue: {1 0f 1b}, {6 0f 1b} MainQueue:  GhostQueue: 3; TryKeepInMemoryTier: SmallQueue: {2 0f 2b}, {4 0f 4b}, {5 0f 5b} MainQueue:  GhostQueue: ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 13);
        CheckStats(cache, {.RegularBytes = 2, .TryKeepInMemoryBytes = 11, .RegularLimit = 2, .TryKeepInMemoryLimit = 11});

        cache.UpdateLimit(6, 11);
        CheckStats(cache, {.RegularBytes = 2, .TryKeepInMemoryBytes = 11, .RegularLimit = 0, .TryKeepInMemoryLimit = 6});
    }
}

} // namespace NKikimr::NSharedCache
