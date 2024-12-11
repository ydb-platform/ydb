#include "cache_cache.h"
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/xrange.h>

namespace NKikimr::NCache {

Y_UNIT_TEST_SUITE(TCacheCacheTest) {

    struct TPage : public TIntrusiveListItem<TPage> {
        ECacheCacheGeneration CacheGeneration;
    };

    struct TCacheCachePageTraits {
        static ui64 GetWeight(const TPage*) {
            return 1;
        }

        static ECacheCacheGeneration GetGeneration(const TPage *page) {
            return static_cast<ECacheCacheGeneration>(page->CacheGeneration);
        }

        static void SetGeneration(TPage *page, ECacheCacheGeneration generation) {
            ui32 generation_ = static_cast<ui32>(generation);
            Y_ABORT_UNLESS(generation_ < (1 << 4));
            page->CacheGeneration = generation;
        }
    };

    Y_UNIT_TEST(MoveToWarm) {
        TCacheCacheConfig::TCounterPtr fresh = new NMonitoring::TCounterForPtr;
        TCacheCacheConfig::TCounterPtr staging = new NMonitoring::TCounterForPtr;
        TCacheCacheConfig::TCounterPtr warm = new NMonitoring::TCounterForPtr;
        // use limit 1 which translates to limit 0 at each level
        // this should mean nothing is cacheable, but currently we will
        // place 1 page on a level until it is inspected again.
        TCacheCacheConfig config(1, fresh, staging, warm);
        TCacheCache<TPage, TCacheCachePageTraits> cache(config);

        TVector<TPage> pages(3);
        TIntrusiveList<TPage> evicted;

        // page 0 added to fresh
        evicted = cache.Touch(&pages[0]);
        UNIT_ASSERT(pages[0].CacheGeneration == ECacheCacheGeneration::Fresh);
        UNIT_ASSERT_VALUES_EQUAL(fresh->Val(), 1ULL);
        UNIT_ASSERT_VALUES_EQUAL(staging->Val(), 0ULL);
        UNIT_ASSERT_VALUES_EQUAL(warm->Val(), 0ULL);
        UNIT_ASSERT(evicted.Empty());

        // page 1 added to fresh first bumps page 0 to staging 
        evicted = cache.Touch(&pages[1]);
        UNIT_ASSERT(pages[1].CacheGeneration == ECacheCacheGeneration::Fresh);
        UNIT_ASSERT(pages[0].CacheGeneration == ECacheCacheGeneration::Staging);
        UNIT_ASSERT_VALUES_EQUAL(fresh->Val(), 1ULL);
        UNIT_ASSERT_VALUES_EQUAL(staging->Val(), 1ULL);
        UNIT_ASSERT_VALUES_EQUAL(warm->Val(), 0ULL);
        UNIT_ASSERT(evicted.Empty());

        // page 0 is moved to warm from staging
        evicted = cache.Touch(&pages[0]);
        UNIT_ASSERT(pages[0].CacheGeneration == ECacheCacheGeneration::Warm);
        UNIT_ASSERT_VALUES_EQUAL(fresh->Val(), 1ULL);
        UNIT_ASSERT_VALUES_EQUAL(staging->Val(), 0ULL);
        UNIT_ASSERT_VALUES_EQUAL(warm->Val(), 1ULL);
        UNIT_ASSERT(evicted.Empty());

        // page 2 added to fresh first bumps page 1 to staging
        evicted = cache.Touch(&pages[2]);
        UNIT_ASSERT(pages[2].CacheGeneration == ECacheCacheGeneration::Fresh);
        UNIT_ASSERT(pages[1].CacheGeneration == ECacheCacheGeneration::Staging);
        UNIT_ASSERT_VALUES_EQUAL(fresh->Val(), 1ULL);
        UNIT_ASSERT_VALUES_EQUAL(staging->Val(), 1ULL);
        UNIT_ASSERT_VALUES_EQUAL(warm->Val(), 1ULL);
        UNIT_ASSERT(evicted.Empty());

        // page 1 moves to warm, but first it bumps page 0 to staging
        evicted = cache.Touch(&pages[1]);
        UNIT_ASSERT(pages[1].CacheGeneration == ECacheCacheGeneration::Warm);
        UNIT_ASSERT(pages[0].CacheGeneration == ECacheCacheGeneration::Staging);
        UNIT_ASSERT_VALUES_EQUAL(fresh->Val(), 1ULL);
        UNIT_ASSERT_VALUES_EQUAL(staging->Val(), 1ULL);
        UNIT_ASSERT_VALUES_EQUAL(warm->Val(), 1ULL);
        UNIT_ASSERT(evicted.Empty());
    }

    Y_UNIT_TEST(EvictNext) {
        TCacheCacheConfig::TCounterPtr fresh = new NMonitoring::TCounterForPtr;
        TCacheCacheConfig::TCounterPtr staging = new NMonitoring::TCounterForPtr;
        TCacheCacheConfig::TCounterPtr warm = new NMonitoring::TCounterForPtr;
        
        // 2 pages per layer
        TCacheCacheConfig config(3, fresh, staging, warm);
        TCacheCache<TPage, TCacheCachePageTraits> cache(config);

        TVector<TPage> pages(6);

        cache.Touch(&pages[0]);
        cache.Touch(&pages[1]);
        cache.Touch(&pages[2]);
        cache.Touch(&pages[3]);
        cache.Touch(&pages[0]);
        cache.Touch(&pages[1]);
        cache.Touch(&pages[4]);
        cache.Touch(&pages[5]);
        UNIT_ASSERT(pages[0].CacheGeneration == ECacheCacheGeneration::Warm);
        UNIT_ASSERT(pages[1].CacheGeneration == ECacheCacheGeneration::Warm);
        UNIT_ASSERT(pages[2].CacheGeneration == ECacheCacheGeneration::Staging);
        UNIT_ASSERT(pages[3].CacheGeneration == ECacheCacheGeneration::Staging);
        UNIT_ASSERT(pages[4].CacheGeneration == ECacheCacheGeneration::Fresh);
        UNIT_ASSERT(pages[5].CacheGeneration == ECacheCacheGeneration::Fresh);
        UNIT_ASSERT_VALUES_EQUAL(fresh->Val(), 2ULL);
        UNIT_ASSERT_VALUES_EQUAL(warm->Val(), 2ULL);
        UNIT_ASSERT_VALUES_EQUAL(staging->Val(), 2ULL);

        UNIT_ASSERT_VALUES_EQUAL(cache.EvictNext().Front(), &pages[2]);
        UNIT_ASSERT(pages[2].CacheGeneration == ECacheCacheGeneration::None);
        UNIT_ASSERT_VALUES_EQUAL(staging->Val(), 1ULL);

        UNIT_ASSERT_VALUES_EQUAL(cache.EvictNext().Front(), &pages[3]);
        UNIT_ASSERT(pages[3].CacheGeneration == ECacheCacheGeneration::None);
        UNIT_ASSERT_VALUES_EQUAL(staging->Val(), 0ULL);

        UNIT_ASSERT_VALUES_EQUAL(cache.EvictNext().Front(), &pages[4]);
        UNIT_ASSERT(pages[4].CacheGeneration == ECacheCacheGeneration::None);
        UNIT_ASSERT_VALUES_EQUAL(fresh->Val(), 1ULL);

        UNIT_ASSERT_VALUES_EQUAL(cache.EvictNext().Front(), &pages[5]);
        UNIT_ASSERT(pages[5].CacheGeneration == ECacheCacheGeneration::None);
        UNIT_ASSERT_VALUES_EQUAL(fresh->Val(), 0ULL);

        UNIT_ASSERT_VALUES_EQUAL(cache.EvictNext().Front(), &pages[0]);
        UNIT_ASSERT(pages[0].CacheGeneration == ECacheCacheGeneration::None);
        UNIT_ASSERT_VALUES_EQUAL(warm->Val(), 1ULL);

        UNIT_ASSERT_VALUES_EQUAL(cache.EvictNext().Front(), &pages[1]);
        UNIT_ASSERT(pages[1].CacheGeneration == ECacheCacheGeneration::None);
        UNIT_ASSERT_VALUES_EQUAL(warm->Val(), 0ULL);

        UNIT_ASSERT(cache.EvictNext().Empty());
    }

    Y_UNIT_TEST(Random) {
        TCacheCacheConfig::TCounterPtr fresh = new NMonitoring::TCounterForPtr;
        TCacheCacheConfig::TCounterPtr staging = new NMonitoring::TCounterForPtr;
        TCacheCacheConfig::TCounterPtr warm = new NMonitoring::TCounterForPtr;
        
        TCacheCacheConfig config(100, fresh, staging, warm);
        TCacheCache<TPage, TCacheCachePageTraits> cache(config);

        TVector<TPage> pages(500);
        
        ui32 hits = 0, misses = 0;

        for (ui32 i = 0; i < 100000; i++) {
            ui32 pageId = std::sqrt(RandomNumber<ui32>(pages.size() * pages.size()));
            TPage* page = &pages[pageId];
            if (page->CacheGeneration != ECacheCacheGeneration::None) {
                hits++;
            } else {
                misses++;
            }
            cache.Touch(page);
        }

        Cerr << 1.0 * hits / (hits + misses) << Endl;
    }
}

} // namespace NKikimr
