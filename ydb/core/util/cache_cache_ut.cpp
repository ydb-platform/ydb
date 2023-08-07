#include "cache_cache.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TCacheCacheTest) {

    struct TPage : public TIntrusiveListItem<TPage> {
        TCacheCacheConfig::ECacheGeneration CacheGeneration = TCacheCacheConfig::CacheGenNone;
    };

    Y_UNIT_TEST(MoveToWarm) {
        TCacheCacheConfig::TCounterPtr fresh = new NMonitoring::TCounterForPtr;
        TCacheCacheConfig::TCounterPtr staging = new NMonitoring::TCounterForPtr;
        TCacheCacheConfig::TCounterPtr warm = new NMonitoring::TCounterForPtr;
        // use limit 1 which translates to limit 0 at each level
        // this should mean nothing is cacheable, but currently we will
        // place 1 page on a level until it is inspected again.
        TCacheCacheConfig config(1, fresh, staging, warm);
        TCacheCache<TPage> cache(config);

        TVector<TPage> pages(3);
        TIntrusiveList<TPage> evicted;

        // page 0 added to fresh
        evicted = cache.Touch(&pages[0]);
        UNIT_ASSERT(pages[0].CacheGeneration == TCacheCacheConfig::CacheGenFresh);
        UNIT_ASSERT_VALUES_EQUAL(fresh->Val(), 1ULL);
        UNIT_ASSERT_VALUES_EQUAL(staging->Val(), 0ULL);
        UNIT_ASSERT_VALUES_EQUAL(warm->Val(), 0ULL);
        UNIT_ASSERT(evicted.Empty());

        // page 1 added to fresh first bumps page 0 to staging 
        evicted = cache.Touch(&pages[1]);
        UNIT_ASSERT(pages[1].CacheGeneration == TCacheCacheConfig::CacheGenFresh);
        UNIT_ASSERT(pages[0].CacheGeneration == TCacheCacheConfig::CacheGenStaging);
        UNIT_ASSERT_VALUES_EQUAL(fresh->Val(), 1ULL);
        UNIT_ASSERT_VALUES_EQUAL(staging->Val(), 1ULL);
        UNIT_ASSERT_VALUES_EQUAL(warm->Val(), 0ULL);
        UNIT_ASSERT(evicted.Empty());

        // page 0 is moved to warm from staging
        evicted = cache.Touch(&pages[0]);
        UNIT_ASSERT(pages[0].CacheGeneration == TCacheCacheConfig::CacheGenWarm);
        UNIT_ASSERT_VALUES_EQUAL(fresh->Val(), 1ULL);
        UNIT_ASSERT_VALUES_EQUAL(staging->Val(), 0ULL);
        UNIT_ASSERT_VALUES_EQUAL(warm->Val(), 1ULL);
        UNIT_ASSERT(evicted.Empty());

        // page 2 added to fresh first bumps page 1 to staging
        evicted = cache.Touch(&pages[2]);
        UNIT_ASSERT(pages[2].CacheGeneration == TCacheCacheConfig::CacheGenFresh);
        UNIT_ASSERT(pages[1].CacheGeneration == TCacheCacheConfig::CacheGenStaging);
        UNIT_ASSERT_VALUES_EQUAL(fresh->Val(), 1ULL);
        UNIT_ASSERT_VALUES_EQUAL(staging->Val(), 1ULL);
        UNIT_ASSERT_VALUES_EQUAL(warm->Val(), 1ULL);
        UNIT_ASSERT(evicted.Empty());

        // page 1 moves to warm, but first it bumps page 0 to staging
        evicted = cache.Touch(&pages[1]);
        UNIT_ASSERT(pages[1].CacheGeneration == TCacheCacheConfig::CacheGenWarm);
        UNIT_ASSERT(pages[0].CacheGeneration == TCacheCacheConfig::CacheGenStaging);
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
        TCacheCache<TPage, TCacheCacheConfig::TDefaultWeight<TPage>, TCacheCacheConfig::TDefaultGeneration<TPage>> cache(config);

        TVector<TPage> pages(6);

        cache.Touch(&pages[0]);
        cache.Touch(&pages[1]);
        cache.Touch(&pages[2]);
        cache.Touch(&pages[3]);
        cache.Touch(&pages[0]);
        cache.Touch(&pages[1]);
        cache.Touch(&pages[4]);
        cache.Touch(&pages[5]);
        UNIT_ASSERT(pages[0].CacheGeneration == TCacheCacheConfig::CacheGenWarm);
        UNIT_ASSERT(pages[1].CacheGeneration == TCacheCacheConfig::CacheGenWarm);
        UNIT_ASSERT(pages[2].CacheGeneration == TCacheCacheConfig::CacheGenStaging);
        UNIT_ASSERT(pages[3].CacheGeneration == TCacheCacheConfig::CacheGenStaging);
        UNIT_ASSERT(pages[4].CacheGeneration == TCacheCacheConfig::CacheGenFresh);
        UNIT_ASSERT(pages[5].CacheGeneration == TCacheCacheConfig::CacheGenFresh);
        UNIT_ASSERT_VALUES_EQUAL(fresh->Val(), 2ULL);
        UNIT_ASSERT_VALUES_EQUAL(warm->Val(), 2ULL);
        UNIT_ASSERT_VALUES_EQUAL(staging->Val(), 2ULL);

        UNIT_ASSERT_VALUES_EQUAL(cache.EvictNext(), &pages[2]);
        UNIT_ASSERT(pages[2].CacheGeneration == TCacheCacheConfig::CacheGenEvicted);
        UNIT_ASSERT_VALUES_EQUAL(staging->Val(), 1ULL);

        UNIT_ASSERT_VALUES_EQUAL(cache.EvictNext(), &pages[3]);
        UNIT_ASSERT(pages[3].CacheGeneration == TCacheCacheConfig::CacheGenEvicted);
        UNIT_ASSERT_VALUES_EQUAL(staging->Val(), 0ULL);

        UNIT_ASSERT_VALUES_EQUAL(cache.EvictNext(), &pages[4]);
        UNIT_ASSERT(pages[4].CacheGeneration == TCacheCacheConfig::CacheGenEvicted);
        UNIT_ASSERT_VALUES_EQUAL(fresh->Val(), 1ULL);

        UNIT_ASSERT_VALUES_EQUAL(cache.EvictNext(), &pages[5]);
        UNIT_ASSERT(pages[5].CacheGeneration == TCacheCacheConfig::CacheGenEvicted);
        UNIT_ASSERT_VALUES_EQUAL(fresh->Val(), 0ULL);

        UNIT_ASSERT_VALUES_EQUAL(cache.EvictNext(), &pages[0]);
        UNIT_ASSERT(pages[0].CacheGeneration == TCacheCacheConfig::CacheGenEvicted);
        UNIT_ASSERT_VALUES_EQUAL(warm->Val(), 1ULL);

        UNIT_ASSERT_VALUES_EQUAL(cache.EvictNext(), &pages[1]);
        UNIT_ASSERT(pages[1].CacheGeneration == TCacheCacheConfig::CacheGenEvicted);
        UNIT_ASSERT_VALUES_EQUAL(warm->Val(), 0ULL);

        UNIT_ASSERT_VALUES_EQUAL(cache.EvictNext(), nullptr);
    }
}

} // namespace NKikimr
