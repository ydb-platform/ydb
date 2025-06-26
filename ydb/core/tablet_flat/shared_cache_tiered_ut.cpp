#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tablet_flat/shared_cache_switchable.h>
#include <ydb/core/tablet_flat/shared_cache_tiered.h>
#include <ydb/core/tablet_flat/ut/shared_cache_ut_common.h>


namespace NKikimr::NSharedCache {

using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
using namespace NKikimr::NSharedCache::NTest;

Y_UNIT_TEST_SUITE(TieredCache) {

    TVector<ui32> Touch(auto& cache, TPage& page, ui32 tierHint) {
        auto evicted = cache.Touch(&page, tierHint);
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheTier, MaxCacheTier);
            result.push_back(p.Id);
        }
        return result;
    }

    TVector<ui32> MoveTouch(auto& cache, TPage& page, ui32 tier) {
        auto evicted = cache.MoveTouch(&page, tier);
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheTier, MaxCacheTier);
            result.push_back(p.Id);
        }
        return result;
    }

    void Erase(auto& cache, TPage& page) {
        cache.Erase(&page);
        UNIT_ASSERT_VALUES_EQUAL(page.CacheTier, MaxCacheTier);
    }

    TVector<ui32> TryMove(auto& cache, TPage& page, ui32 tier) {
        auto evicted = cache.TryMove(&page, tier);
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheTier, MaxCacheTier);
            result.push_back(p.Id);
        }
        return result;
    }

    TVector<ui32> EvictNext(auto& cache) {
        auto evicted = cache.EvictNext();
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheTier, MaxCacheTier);
            result.push_back(p.Id);
        }
        return result;
    }


    TVector<ui32> Switch(auto& cache, auto&& cache2, auto& counter) {
        auto evicted = cache.Switch(std::move(cache2), counter);
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheId, MaxCacheTier);
            result.push_back(p.Id);
        }
        return result;
    }

    Y_UNIT_TEST(Touch) {
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());

        TVector<TSimpleCache*> caches;
        auto makeCache = [&caches]() {
            auto cacheHolder = MakeHolder<TSimpleCache>();
            caches.push_back(cacheHolder.Get());
            return cacheHolder;
        };

        TTieredCache<TPage, TPageTraits> cache(10, makeCache, 2, NKikimrSharedCache::S3FIFO, counters);
        UNIT_ASSERT_VALUES_EQUAL(caches.size(), 2);

        TPage page1{1, 2};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 0), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 2b}; Tier 1: ");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 10);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 0);

        TPage page2{2, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 2b}; Tier 1: {2 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 7);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 3);

        TPage page3{3, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3, 0), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 2b}, {3 3b}; Tier 1: {2 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 7);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 3);

        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4, 0), (TVector<ui32>{1}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {3 3b}, {4 4b}; Tier 1: {2 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 7);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 7);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 3);

        TPage page5{5, 2};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5, 0), (TVector<ui32>{3}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {4 4b}, {5 2b}; Tier 1: {2 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 7);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 3);

        TPage page6{6, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page6, 1), (TVector<ui32>{4}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {5 2b}; Tier 1: {2 3b}, {6 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page6, 0), (TVector<ui32>{}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {5 2b}; Tier 1: {2 3b}, {6 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page6, 0), (TVector<ui32>{}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {5 2b}, {6 3b}; Tier 1: {2 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 7);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 3);
    }

    Y_UNIT_TEST(Touch3Tiers) {
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());

        TVector<TSimpleCache*> caches;
        auto makeCache = [&caches]() {
            auto cacheHolder = MakeHolder<TSimpleCache>();
            caches.push_back(cacheHolder.Get());
            return cacheHolder;
        };

        TTieredCache<TPage, TPageTraits> cache(10, makeCache, 3, NKikimrSharedCache::S3FIFO, counters);
        UNIT_ASSERT_VALUES_EQUAL(caches.size(), 3);

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 0), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3, 2), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 1b}; Tier 1: {2 2b}; Tier 2: {3 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 5);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 2);
        UNIT_ASSERT_VALUES_EQUAL(caches[2]->GetLimit(), 3);

        Erase(cache, page2);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 1b}; Tier 1: ; Tier 2: {3 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 7);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 0);
        UNIT_ASSERT_VALUES_EQUAL(caches[2]->GetLimit(), 3);

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 1b}; Tier 1: ; Tier 2: {3 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 7);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 0);
        UNIT_ASSERT_VALUES_EQUAL(caches[2]->GetLimit(), 3);

        UNIT_ASSERT_VALUES_EQUAL(MoveTouch(cache, page1, 2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: ; Tier 1: ; Tier 2: {3 3b}, {1 1b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 0);
        UNIT_ASSERT_VALUES_EQUAL(caches[2]->GetLimit(), 4);
    }

    Y_UNIT_TEST(Move) {
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());

        TVector<TSimpleCache*> caches;
        auto makeCache = [&caches]() {
            auto cacheHolder = MakeHolder<TSimpleCache>();
            caches.push_back(cacheHolder.Get());
            return cacheHolder;
        };

        TTieredCache<TPage, TPageTraits> cache(10, makeCache, 2, NKikimrSharedCache::S3FIFO, counters);
        UNIT_ASSERT_VALUES_EQUAL(caches.size(), 2);

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 0), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3, 0), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4, 1), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 1b}, {3 3b}; Tier 1: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page2, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 1b}, {3 3b}; Tier 1: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page3, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 1b}; Tier 1: {2 2b}, {4 4b}, {3 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 9);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 1);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 9);

        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page1, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: ; Tier 1: {2 2b}, {4 4b}, {3 3b}, {1 1b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 10);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 10);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 0);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 10);

        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page4, 0), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {4 4b}; Tier 1: {2 2b}, {3 3b}, {1 1b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page1, 0), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page2, 0), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page3, 0), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {4 4b}, {1 1b}, {2 2b}, {3 3b}; Tier 1: ");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 10);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 10);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 10);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 0);
    }

    Y_UNIT_TEST(Erase) {
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());

        TVector<TSimpleCache*> caches;
        auto makeCache = [&caches]() {
            auto cacheHolder = MakeHolder<TSimpleCache>();
            caches.push_back(cacheHolder.Get());
            return cacheHolder;
        };

        TTieredCache<TPage, TPageTraits> cache(10, makeCache, 2, NKikimrSharedCache::S3FIFO, counters);
        UNIT_ASSERT_VALUES_EQUAL(caches.size(), 2);

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 0), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3, 0), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 1b}, {3 3b}; Tier 1: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);
        
        Erase(cache, page2);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 1b}, {3 3b}; Tier 1: {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 10);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 4);

        TPage page5{5, 4};
        Erase(cache, page5);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 1b}, {3 3b}; Tier 1: {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 4);

        Erase(cache, page3);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 1b}; Tier 1: {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 10);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 4);
    }

    Y_UNIT_TEST(EvictNext) {
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());

        TVector<TSimpleCache*> caches;
        auto makeCache = [&caches]() {
            auto cacheHolder = MakeHolder<TSimpleCache>();
            caches.push_back(cacheHolder.Get());
            return cacheHolder;
        };

        TTieredCache<TPage, TPageTraits> cache(10, makeCache, 2, NKikimrSharedCache::S3FIFO, counters);
        UNIT_ASSERT_VALUES_EQUAL(caches.size(), 2);

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 0), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3, 0), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4, 1), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 1b}, {3 3b}; Tier 1: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{1});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {3 3b}; Tier 1: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{3});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: ; Tier 1: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{2});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: ; Tier 1: {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 4);

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{4});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: ; Tier 1: ");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 10);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 0);
    }

    Y_UNIT_TEST(UpdateLimit) {
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());

        TVector<TSimpleCache*> caches;
        auto makeCache = [&caches]() {
            auto cacheHolder = MakeHolder<TSimpleCache>();
            caches.push_back(cacheHolder.Get());
            return cacheHolder;
        };

        TTieredCache<TPage, TPageTraits> cache(10, makeCache, 2, NKikimrSharedCache::S3FIFO, counters);
        UNIT_ASSERT_VALUES_EQUAL(caches.size(), 2);

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3, 0), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 0), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4, 1), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {3 3b}, {1 1b}; Tier 1: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 10);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        cache.UpdateLimit(15);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {3 3b}, {1 1b}; Tier 1: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 15);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 9);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        TPage page5{5, 5};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5, 0), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {3 3b}, {1 1b}, {5 5b}; Tier 1: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 9);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 15);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 9);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(TryMove(cache, page5, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {3 3b}, {1 1b}; Tier 1: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 15);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 11);

        cache.UpdateLimit(13);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {3 3b}, {1 1b}; Tier 1: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 13);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 2);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 11);

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 0), TVector<ui32>{3});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 1b}; Tier 1: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 13);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 2);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 11);

        cache.UpdateLimit(6);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 1b}; Tier 1: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 0);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 0), (TVector<ui32>{1}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: ; Tier 1: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 0);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5, 1), (TVector<ui32>{2, 4}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: ; Tier 1: {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 1);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 5);
    }

    Y_UNIT_TEST(Switch) {
        TCounterPtr sizeCounter2 = MakeIntrusive<NMonitoring::TCounterForPtr>();
        TSharedPageCacheCounters counters(MakeIntrusive<NMonitoring::TDynamicCounters>());

        TVector<TSimpleCache*> caches;
        auto makeCache = [&caches]() {
            auto cacheHolder = MakeHolder<TSimpleCache>();
            caches.push_back(cacheHolder.Get());
            return cacheHolder;
        };

        TTieredCache<TPage, TPageTraits> cache(10, makeCache, 2, NKikimrSharedCache::S3FIFO, counters);
        UNIT_ASSERT_VALUES_EQUAL(caches.size(), 2);

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 0), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3, 0), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 1b}, {3 3b}; Tier 1: {2 2b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ReplacementPolicySize(NKikimrSharedCache::S3FIFO)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(sizeCounter2->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 8);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 2);

        UNIT_ASSERT_VALUES_EQUAL(Switch(cache, makeCache, sizeCounter2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(caches.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 1b}, {3 3b}; ; Tier 1: {2 2b}; ");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ReplacementPolicySize(NKikimrSharedCache::S3FIFO)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(sizeCounter2->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 8);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 2);
        UNIT_ASSERT_VALUES_EQUAL(caches[2]->GetLimit(), 8);
        UNIT_ASSERT_VALUES_EQUAL(caches[3]->GetLimit(), 2);

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 0), TVector<ui32>{});
        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 0: {1 1b}, {3 3b}; Tier 1: {4 4b}, {2 2b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val() + counters.ActiveBytesTier(1)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ReplacementPolicySize(NKikimrSharedCache::S3FIFO)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sizeCounter2->Val(), 10);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(0)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[2]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[3]->GetLimit(), 6);
    }

}

} // namespace NKikimr::NSharedCache
