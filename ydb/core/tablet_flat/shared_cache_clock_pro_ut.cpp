#include <library/cpp/testing/unittest/registar.h>
#include <util/random/mersenne.h>
#include "shared_cache_clock_pro.h"

namespace NKikimr::NCache {

namespace {

    struct TPage : public TIntrusiveListItem<TPage> {
        ui32 Id;
        size_t Size;

        TPage(ui32 id, size_t size) 
            : Id(id), Size(size)
        {}

        ui32 CacheFlags1 : 4 = 0;
        ui32 CacheFlags2 : 4 = 0;
    };

    struct TPageTraits {
        struct TPageKey {
            ui32 Id;

            TPageKey(ui32 id)
                : Id(id)
            {}
        };
        
        static ui64 GetSize(const TPage* page) {
            return page->Size;
        }

        static TPageKey GetKey(const TPage* page) {
            return {page->Id};
        }

        static size_t GetHash(const TPageKey& key) {
            return std::hash<ui32>()(key.Id);
        }

        static bool Equals(const TPageKey& left, const TPageKey& right) {
            return left.Id == right.Id;
        }

        static TString ToString(const TPageKey& key) {
            return std::to_string(key.Id);
        }

        static TString GetKeyToString(const TPage* page) {
            return ToString(GetKey(page));
        }

        static EClockProPageLocation GetLocation(const TPage* page) {
            return static_cast<EClockProPageLocation>(page->CacheFlags1);
        }

        static void SetLocation(TPage* page, EClockProPageLocation location) {
            ui32 location_ = static_cast<ui32>(location);
            Y_ABORT_UNLESS(location_ < (1 << 4));
            page->CacheFlags1 = location_;
        }

        static bool GetReferenced(const TPage* page) {
            return page->CacheFlags2;
        }

        static void SetReferenced(TPage* page, bool referenced) {
            page->CacheFlags2 = referenced;
        }
    };

}

Y_UNIT_TEST_SUITE(TClockProCache) {

    TVector<ui32> Touch(auto& cache, TPage& page) {
        auto evicted = cache.Touch(&page);
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheFlags1, 0);
            UNIT_ASSERT_VALUES_EQUAL(p.CacheFlags2, 0);
            result.push_back(p.Id);
        }
        return result;
    }

    TVector<ui32> EvictNext(auto& cache) {
        auto evicted = cache.EvictNext();
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheFlags1, 0);
            UNIT_ASSERT_VALUES_EQUAL(p.CacheFlags2, 0);
            result.push_back(p.Id);
        }
        return result;
    }

    Y_UNIT_TEST(Touch) {
        TClockProCache<TPage, TPageTraits> cache(10);

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "ColdTarget: 10");

        TPage page1{1, 2};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Cold>Test>{1 C 0r 2b}; ColdTarget: 10");
        
        TPage page2{2, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{1 C 0r 2b}, Cold>{2 C 0r 3b}; ColdTarget: 10");

        TPage page3{3, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{1 C 0r 2b}, Cold>{2 C 0r 3b}, {3 C 0r 4b}; ColdTarget: 10");

        TPage page4{4, 1};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{1 C 0r 2b}, Cold>{2 C 0r 3b}, {3 C 0r 4b}, {4 C 0r 1b}; ColdTarget: 10");
        
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{1 C 1r 2b}, Cold>{2 C 0r 3b}, {3 C 1r 4b}, {4 C 0r 1b}; ColdTarget: 10");

        TPage page5{5, 1};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), TVector<ui32>{2});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{1 C 1r 2b}, {2 T 3b}, Cold>{3 C 1r 4b}, {4 C 0r 1b}, {5 C 0r 1b}; ColdTarget: 10");
    }

    Y_UNIT_TEST(Lifecycle) {
        TClockProCache<TPage, TPageTraits> cache(10);

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "ColdTarget: 10");

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        TPage page4{4, 4};
        TPage page5{5, 5};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{1 C 0r 1b}, Cold>{2 C 0r 2b}, {3 C 0r 3b}, {4 C 0r 4b}; ColdTarget: 10");

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{1 C 0r 1b}, Cold>{2 C 1r 2b}, {3 C 0r 3b}, {4 C 0r 4b}; ColdTarget: 10");

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), (TVector<ui32>{3, 4}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{3 T 3b}, {4 T 4b}, Cold>{5 C 0r 5b}, {1 C 0r 1b}, {2 C 0r 2b}; ColdTarget: 10");

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), (TVector<ui32>{5}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{5 T 5b}, Cold>{1 C 0r 1b}, {3 H 0r 3b}, {2 C 0r 2b}; ColdTarget: 6");

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{5 T 5b}, Cold>{1 C 0r 1b}, {3 H 1r 3b}, {2 C 0r 2b}; ColdTarget: 6");

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{5 T 5b}, Cold>{1 C 0r 1b}, {3 H 1r 3b}, {2 C 0r 2b}, {4 C 0r 4b}; ColdTarget: 6");

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), (TVector<ui32>{1, 2, 4}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{4 T 4b}, Cold>{3 C 0r 3b}, {5 C 0r 5b}; ColdTarget: 7");
    }

    Y_UNIT_TEST(EvictNext) {
        TClockProCache<TPage, TPageTraits> cache(10);

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "ColdTarget: 10");

        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        TPage page4{4, 4};
        TPage page5{5, 5};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{1 C 0r 1b}, Cold>{2 C 0r 2b}, {3 C 0r 3b}, {4 C 0r 4b}; ColdTarget: 10");

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{1 C 0r 1b}, Cold>{2 C 1r 2b}, {3 C 0r 3b}, {4 C 0r 4b}; ColdTarget: 10");

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), (TVector<ui32>{3, 4}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{3 T 3b}, {4 T 4b}, Cold>{5 C 0r 5b}, {1 C 0r 1b}, {2 C 0r 2b}; ColdTarget: 10");

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), (TVector<ui32>{5}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{5 T 5b}, Cold>{1 C 0r 1b}, {3 H 0r 3b}, {2 C 0r 2b}; ColdTarget: 6");

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{1});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>{2 C 0r 2b}, Test>{1 T 1b}, Cold>{3 H 0r 3b}; ColdTarget: 6");

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{2});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{2 T 2b}, Cold>{3 C 0r 3b}; ColdTarget: 6");

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{3});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "ColdTarget: 6");

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "ColdTarget: 6");
    }

    Y_UNIT_TEST(UpdateLimit) {
        TClockProCache<TPage, TPageTraits> cache(10);
        
        TPage page1{1, 1};
        TPage page2{2, 2};
        TPage page3{3, 3};
        TPage page4{4, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{1 C 0r 1b}, Cold>{2 C 0r 2b}, {3 C 0r 3b}, {4 C 0r 4b}; ColdTarget: 10");

        cache.UpdateLimit(5);
        TPage page5{5, 1};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), (TVector<ui32>{2, 3, 4}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>{1 C 0r 1b}, Test>{4 T 4b}, Cold>{5 C 0r 1b}; ColdTarget: 0");

        cache.UpdateLimit(0);
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), (TVector<ui32>{5, 1, 2}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "ColdTarget: 0");
    }

    Y_UNIT_TEST(Erase) {
        TClockProCache<TPage, TPageTraits> cache(10);

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "ColdTarget: 10");

        TPage page1{1, 2};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Cold>Test>{1 C 0r 2b}; ColdTarget: 10");
        
        TPage page2{2, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{1 C 0r 2b}, Cold>{2 C 0r 3b}; ColdTarget: 10");

        TPage page3{3, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{1 C 0r 2b}, Cold>{2 C 0r 3b}, {3 C 0r 4b}; ColdTarget: 10");

        cache.Erase(&page1);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{3 C 0r 4b}, Cold>{2 C 0r 3b}; ColdTarget: 10");

        cache.Erase(&page2);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Cold>Test>{3 C 0r 4b}; ColdTarget: 10");
        
        TPage page42{42, 1};
        cache.Erase(&page42);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Cold>Test>{3 C 0r 4b}; ColdTarget: 10");
        
        cache.Erase(&page3);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "ColdTarget: 10");
    }

    Y_UNIT_TEST(Random) {
        TClockProCache<TPage, TPageTraits> cache(100);

        TVector<THolder<TPage>> pages;
        for (ui32 pageId : xrange(500)) {
            pages.push_back(MakeHolder<TPage>(pageId, 1));
        }

        ui32 hits = 0, misses = 0;

        for (ui32 i = 0; i < 100000; i++) {
            ui32 pageId = std::sqrt(RandomNumber<ui32>(pages.size() * pages.size()));
            TPage* page = pages[pageId].Get();
            if (TPageTraits::GetLocation(page) != EClockProPageLocation::None) {
                hits++;
            } else {
                misses++;
            }
            cache.Touch(page);
        }

        Cerr << 1.0 * hits / (hits + misses) << Endl;
    }
}

}
