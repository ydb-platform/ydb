#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tablet_flat/shared_cache_switchable.h>
#include <ydb/core/tablet_flat/shared_cache_tiered.h>
#include <ydb/core/tablet_flat/ut/shared_cache_ut_common.h>


namespace NKikimr::NSharedCache {

using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
using namespace NKikimr::NSharedCache::NTest;

Y_UNIT_TEST_SUITE(TieredCache) {

    class TTieredTestCounters : public ITieredCacheCounters {
    public:
        TTieredTestCounters(ui32 numberOfTiers) {
            for (auto _ : xrange(numberOfTiers)) {
                ActivePagesCounters.push_back(MakeIntrusive<NMonitoring::TCounterForPtr>());
                ActiveBytesCounters.push_back(MakeIntrusive<NMonitoring::TCounterForPtr>());
            }
        }

        TCounterPtr ActivePagesTier(ui32 tier) override {
            return ActivePagesCounters[tier - 1];
        }

        TCounterPtr ActiveBytesTier(ui32 tier) override {
            return ActiveBytesCounters[tier - 1];
        }
    
    private:
        TVector<TCounterPtr> ActivePagesCounters;
        TVector<TCounterPtr> ActiveBytesCounters;
    };

    TVector<ui32> Touch(auto& cache, TPage& page, ui32 tier) {
        auto evicted = cache.Touch(&page, tier);
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheTier, 0);
            result.push_back(p.Id);
        }
        return result;
    }

    void Erase(auto& cache, TPage& page) {
        cache.Erase(&page);
        UNIT_ASSERT_VALUES_EQUAL(page.CacheTier, 0);
    }

    TVector<ui32> Move(auto& cache, TPage& page, ui32 tier) {
        auto evicted = cache.Move(&page, tier);
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheTier, 0);
            result.push_back(p.Id);
        }
        return result;
    }

    TVector<ui32> EvictNext(auto& cache) {
        auto evicted = cache.EvictNext();
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheTier, 0);
            result.push_back(p.Id);
        }
        return result;
    }


    TVector<ui32> Switch(auto& cache, auto&& cache2, auto& counter) {
        auto evicted = cache.Switch(std::move(cache2), counter);
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheId, 0);
            result.push_back(p.Id);
        }
        return result;
    }

    Y_UNIT_TEST(Touch) {
        TCounterPtr sizeCounter = MakeIntrusive<NMonitoring::TCounterForPtr>();
        TTieredTestCounters counters(2);

        TVector<TSimpleCache*> caches;
        auto makeCache = [&caches]() {
            auto cacheHolder = MakeHolder<TSimpleCache>();
            caches.push_back(cacheHolder.Get());
            return cacheHolder;
        };

        TTieredCache<TPage, TPageTraits> cache(10, makeCache, sizeCounter, 2, counters);
        UNIT_ASSERT_VALUES_EQUAL(caches.size(), 2);

        TPage page1{1, 2};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 2b}; Tier 2: ");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 10);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 0);

        TPage page2{2, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2, 2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 2b}; Tier 2: {2 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 7);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 3);

        TPage page3{3, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 2b}, {3 3b}; Tier 2: {2 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 7);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 3);

        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4, 1), (TVector<ui32>{1}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {3 3b}, {4 4b}; Tier 2: {2 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 7);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 7);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 3);

        TPage page5{5, 2};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5, 1), (TVector<ui32>{3}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {4 4b}, {5 2b}; Tier 2: {2 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 7);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 3);

        TPage page6{6, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page6, 2), (TVector<ui32>{4}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {5 2b}; Tier 2: {2 3b}, {6 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page6, 1), (TVector<ui32>{}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {5 2b}, {6 3b}; Tier 2: {2 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 7);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 3);
    }

    Y_UNIT_TEST(Touch3Tiers) {
        TCounterPtr sizeCounter = MakeIntrusive<NMonitoring::TCounterForPtr>();
        TTieredTestCounters counters(3);

        TVector<TSimpleCache*> caches;
        auto makeCache = [&caches]() {
            auto cacheHolder = MakeHolder<TSimpleCache>();
            caches.push_back(cacheHolder.Get());
            return cacheHolder;
        };

        TTieredCache<TPage, TPageTraits> cache(10, makeCache, sizeCounter, 3, counters);
        UNIT_ASSERT_VALUES_EQUAL(caches.size(), 3);

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2, 2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3, 3), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 1b}; Tier 2: {2 2b}; Tier 3: {3 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val() + counters.ActiveBytesTier(3)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(3)->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 5);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 2);
        UNIT_ASSERT_VALUES_EQUAL(caches[2]->GetLimit(), 3);

        Erase(cache, page2);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 1b}; Tier 2: ; Tier 3: {3 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val() + counters.ActiveBytesTier(3)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(3)->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 7);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 0);
        UNIT_ASSERT_VALUES_EQUAL(caches[2]->GetLimit(), 3);

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: ; Tier 2: ; Tier 3: {3 3b}, {1 1b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val() + counters.ActiveBytesTier(3)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(3)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 0);
        UNIT_ASSERT_VALUES_EQUAL(caches[2]->GetLimit(), 4);
    }

    Y_UNIT_TEST(Move) {
        TCounterPtr sizeCounter = MakeIntrusive<NMonitoring::TCounterForPtr>();
        TTieredTestCounters counters(2);

        TVector<TSimpleCache*> caches;
        auto makeCache = [&caches]() {
            auto cacheHolder = MakeHolder<TSimpleCache>();
            caches.push_back(cacheHolder.Get());
            return cacheHolder;
        };

        TTieredCache<TPage, TPageTraits> cache(10, makeCache, sizeCounter, 2, counters);
        UNIT_ASSERT_VALUES_EQUAL(caches.size(), 2);

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2, 2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4, 2), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 1b}, {3 3b}; Tier 2: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(Move(cache, page2, 2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 1b}, {3 3b}; Tier 2: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(Move(cache, page3, 2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 1b}; Tier 2: {2 2b}, {4 4b}, {3 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 9);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 1);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 9);

        UNIT_ASSERT_VALUES_EQUAL(Move(cache, page1, 2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: ; Tier 2: {2 2b}, {4 4b}, {3 3b}, {1 1b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 10);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 10);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 0);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 10);

        UNIT_ASSERT_VALUES_EQUAL(Move(cache, page4, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {4 4b}; Tier 2: {2 2b}, {3 3b}, {1 1b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(Move(cache, page1, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Move(cache, page2, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Move(cache, page3, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {4 4b}, {1 1b}, {2 2b}, {3 3b}; Tier 2: ");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 10);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 10);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 10);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 0);
    }

    Y_UNIT_TEST(Erase) {
        TCounterPtr sizeCounter = MakeIntrusive<NMonitoring::TCounterForPtr>();
        TTieredTestCounters counters(2);

        TVector<TSimpleCache*> caches;
        auto makeCache = [&caches]() {
            auto cacheHolder = MakeHolder<TSimpleCache>();
            caches.push_back(cacheHolder.Get());
            return cacheHolder;
        };

        TTieredCache<TPage, TPageTraits> cache(10, makeCache, sizeCounter, 2, counters);
        UNIT_ASSERT_VALUES_EQUAL(caches.size(), 2);

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2, 2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4, 2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3, 1), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 1b}, {3 3b}; Tier 2: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);
        
        Erase(cache, page2);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 1b}, {3 3b}; Tier 2: {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 10);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 4);

        TPage page5{5, 4};
        Erase(cache, page5);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 1b}, {3 3b}; Tier 2: {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 4);

        Erase(cache, page3);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 1b}; Tier 2: {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 10);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 4);
    }

    Y_UNIT_TEST(EvictNext) {
        TCounterPtr sizeCounter = MakeIntrusive<NMonitoring::TCounterForPtr>();
        TTieredTestCounters counters(2);

        TVector<TSimpleCache*> caches;
        auto makeCache = [&caches]() {
            auto cacheHolder = MakeHolder<TSimpleCache>();
            caches.push_back(cacheHolder.Get());
            return cacheHolder;
        };

        TTieredCache<TPage, TPageTraits> cache(10, makeCache, sizeCounter, 2, counters);
        UNIT_ASSERT_VALUES_EQUAL(caches.size(), 2);

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2, 2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4, 2), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 1b}, {3 3b}; Tier 2: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{1});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {3 3b}; Tier 2: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 3);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{3});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: ; Tier 2: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{2});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: ; Tier 2: {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 4);

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{4});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: ; Tier 2: ");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 10);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 0);
    }

    Y_UNIT_TEST(UpdateLimit) {
        TCounterPtr sizeCounter = MakeIntrusive<NMonitoring::TCounterForPtr>();
        TTieredTestCounters counters(2);

        TVector<TSimpleCache*> caches;
        auto makeCache = [&caches]() {
            auto cacheHolder = MakeHolder<TSimpleCache>();
            caches.push_back(cacheHolder.Get());
            return cacheHolder;
        };

        TTieredCache<TPage, TPageTraits> cache(10, makeCache, sizeCounter, 2, counters);
        UNIT_ASSERT_VALUES_EQUAL(caches.size(), 2);

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2, 2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4, 2), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {3 3b}, {1 1b}; Tier 2: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 10);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        cache.UpdateLimit(15);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {3 3b}, {1 1b}; Tier 2: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 15);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 9);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        TPage page5{5, 5};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {3 3b}, {1 1b}, {5 5b}; Tier 2: {2 2b}, {4 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 9);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 15);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 9);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(Move(cache, page5, 2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {3 3b}, {1 1b}; Tier 2: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 15);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 11);

        cache.UpdateLimit(13);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {3 3b}, {1 1b}; Tier 2: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 13);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 2);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 11);

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 1), TVector<ui32>{3});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 1b}; Tier 2: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 13);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 2);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 11);

        cache.UpdateLimit(6);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 1b}; Tier 2: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 0);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 1), (TVector<ui32>{1}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: ; Tier 2: {2 2b}, {4 4b}, {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 11);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 0);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 6);

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5, 2), (TVector<ui32>{2, 4}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: ; Tier 2: {5 5b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 5);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetLimit(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 1);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 5);
    }

    Y_UNIT_TEST(Switch) {
        TCounterPtr sizeCounter1 = MakeIntrusive<NMonitoring::TCounterForPtr>();
        TCounterPtr sizeCounter2 = MakeIntrusive<NMonitoring::TCounterForPtr>();
        TTieredTestCounters counters(2);

        TVector<TSimpleCache*> caches;
        auto makeCache = [&caches]() {
            auto cacheHolder = MakeHolder<TSimpleCache>();
            caches.push_back(cacheHolder.Get());
            return cacheHolder;
        };

        TTieredCache<TPage, TPageTraits> cache(10, makeCache, sizeCounter1, 2, counters);
        UNIT_ASSERT_VALUES_EQUAL(caches.size(), 2);

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2, 2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3, 1), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 1b}, {3 3b}; Tier 2: {2 2b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(sizeCounter1->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(sizeCounter2->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 8);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 2);

        UNIT_ASSERT_VALUES_EQUAL(Switch(cache, makeCache, sizeCounter2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(caches.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 1b}, {3 3b}; ; Tier 2: {2 2b}; ");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(sizeCounter1->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(sizeCounter2->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(caches[0]->GetLimit(), 8);
        UNIT_ASSERT_VALUES_EQUAL(caches[1]->GetLimit(), 2);
        UNIT_ASSERT_VALUES_EQUAL(caches[2]->GetLimit(), 8);
        UNIT_ASSERT_VALUES_EQUAL(caches[3]->GetLimit(), 2);

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1, 1), TVector<ui32>{});
        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4, 2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Tier 1: {1 1b}, {3 3b}; Tier 2: {4 4b}, {2 2b}");
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val() + counters.ActiveBytesTier(2)->Val(), cache.GetSize());
        UNIT_ASSERT_VALUES_EQUAL(sizeCounter1->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(sizeCounter2->Val(), 10);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(1)->Val(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counters.ActiveBytesTier(2)->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(caches[2]->GetLimit(), 4);
        UNIT_ASSERT_VALUES_EQUAL(caches[3]->GetLimit(), 6);
    }

}

} // namespace NKikimr::NSharedCache
