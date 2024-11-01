#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/resource/resource.h>
#include "shared_cache_s3fifo.h"

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
            return key.Id;
        }

        static TString ToString(const TPageKey& key) {
            return std::to_string(key.Id);
        }

        static TString GetKeyToString(const TPage* page) {
            return ToString(GetKey(page));
        }

        static ES3FIFOPageLocation GetLocation(const TPage* page) {
            return static_cast<ES3FIFOPageLocation>(page->CacheFlags1);
        }

        static void SetLocation(TPage* page, ES3FIFOPageLocation location) {
            ui32 location_ = static_cast<ui32>(location);
            Y_ABORT_UNLESS(location_ < (1 << 4));
            page->CacheFlags1 = location_;
        }

        static ui32 GetFrequency(const TPage* page) {
            return page->CacheFlags2;
        }

        static void SetFrequency(TPage* page, ui32 frequency) {
            Y_ABORT_UNLESS(frequency < (1 << 4));
            page->CacheFlags2 = frequency;
        }
    };

}

Y_UNIT_TEST_SUITE(TS3FIFOGhostQueue) {
    
    Y_UNIT_TEST(Basics) {
        TS3FIFOGhostPageQueue<TPageTraits> queue;
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "");

        // TODO: test Add, Limit, Contains

        UNIT_ASSERT(queue.Add(1));
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "1");

        UNIT_ASSERT(queue.Add(2));
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "1, 2");

        UNIT_ASSERT(queue.Add(3));
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "1, 2, 3");
        UNIT_ASSERT(queue.Contains(1));
        UNIT_ASSERT(queue.Contains(2));
        UNIT_ASSERT(queue.Contains(3));
        UNIT_ASSERT(!queue.Contains(4));

        queue.Limit(2);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "2, 3");
        UNIT_ASSERT(!queue.Contains(1));
        UNIT_ASSERT(queue.Contains(2));
        UNIT_ASSERT(queue.Contains(3));
        UNIT_ASSERT(!queue.Contains(4));

        UNIT_ASSERT(!queue.Add(2));
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "2, 3");
        UNIT_ASSERT(!queue.Contains(1));
        UNIT_ASSERT(queue.Contains(2));
        UNIT_ASSERT(queue.Contains(3));
        UNIT_ASSERT(!queue.Contains(4));
    }

}

Y_UNIT_TEST_SUITE(TS3FIFOCache) {

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

    void Erase(auto& cache, TPage& page) {
        cache.Erase(&page);
        UNIT_ASSERT_VALUES_EQUAL(page.CacheFlags1, 0);
        UNIT_ASSERT_VALUES_EQUAL(page.CacheFlags2, 0);
    }

    Y_UNIT_TEST(Touch) {
        TS3FIFOCache<TPage, TPageTraits> cache(20);

        TPage page1{1, 2};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {1 0f 2b}" << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: "));
        
        TPage page2{2, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {1 0f 2b}, {2 0f 3b}" << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: "));
        
        TPage page3{3, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {1 0f 2b}, {2 0f 3b}, {3 0f 4b}" << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: "));

        TPage page4{4, 1};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {1 0f 2b}, {2 0f 3b}, {3 0f 4b}, {4 0f 1b}" << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: "));
        
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {1 2f 2b}, {2 1f 3b}, {3 2f 4b}, {4 1f 1b}" << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: "));

        TPage page5{5, 12};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), TVector<ui32>{2});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {3 2f 4b}, {4 1f 1b}, {5 0f 12b}" << Endl
            << "MainQueue: {1 0f 2b}" << Endl
            << "GhostQueue: 2"));

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), (TVector<ui32>{4, 5}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {1 0f 2b}, {2 0f 3b}, {3 0f 4b}" << Endl
            << "GhostQueue: 2, 4, 5"));
        
        TPage page6{6, 2};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page6), (TVector<ui32>{}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {6 0f 2b}" << Endl
            << "MainQueue: {1 0f 2b}, {2 0f 3b}, {3 0f 4b}" << Endl
            << "GhostQueue: 2, 4, 5"));
    }

    Y_UNIT_TEST(Touch_MainQueue) {
        TS3FIFOCache<TPage, TPageTraits> cache(10);

        TVector<THolder<TPage>> pages;
        for (ui32 pageId : xrange(20)) {
            pages.push_back(MakeHolder<TPage>(pageId, 1));
        }

        for (ui32 pageId : xrange(10)) {
            for (ui32 times = 0; times <= pageId % 4; times++) {
                Touch(cache, *pages[pageId]);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {0 0f 1b}, {1 1f 1b}, {2 2f 1b}, {3 3f 1b}, {4 0f 1b}, {5 1f 1b}, {6 2f 1b}, {7 3f 1b}, {8 0f 1b}, {9 1f 1b}" << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: "));
        
        for (ui32 pageId : xrange(10)) {
            Touch(cache, *pages[10 + pageId]);
        }
        Touch(cache, *pages[3]);
        Touch(cache, *pages[6]);
        Touch(cache, *pages[6]);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {14 0f 1b}, {15 0f 1b}, {16 0f 1b}, {17 0f 1b}, {18 0f 1b}, {19 0f 1b}" << Endl
            << "MainQueue: {2 0f 1b}, {3 1f 1b}, {6 2f 1b}, {7 0f 1b}" << Endl
            << "GhostQueue: 0, 1, 4, 5, 8, 9, 10, 11, 12, 13"));
        
        for (ui32 pageId : xrange(10)) {
            Touch(cache, *pages[pageId]);
        }
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {19 0f 1b}" << Endl
            << "MainQueue: {1 0f 1b}, {4 0f 1b}, {5 0f 1b}, {8 0f 1b}, {9 0f 1b}, {2 0f 1b}, {3 1f 1b}, {6 2f 1b}, {7 0f 1b}" << Endl
            << "GhostQueue: 9, 10, 11, 12, 13, 14, 15, 16, 17, 18"));
        
        for (ui32 pageId : xrange(10)) {
            Touch(cache, *pages[10 + pageId]);
        }
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {19 1f 1b}" << Endl
            << "MainQueue: {12 0f 1b}, {13 0f 1b}, {14 0f 1b}, {15 0f 1b}, {16 0f 1b}, {3 0f 1b}, {6 1f 1b}, {17 0f 1b}, {18 0f 1b}" << Endl
            << "GhostQueue: 9, 10, 11, 12, 13, 14, 15, 16, 17, 18"));
    }

    Y_UNIT_TEST(EvictNext) {
        TS3FIFOCache<TPage, TPageTraits> cache(10);

        TVector<THolder<TPage>> pages;
        for (ui32 pageId : xrange(30)) {
            pages.push_back(MakeHolder<TPage>(pageId, 1));
        }

        for (ui32 pageId : xrange(30)) {
            for (ui32 times = 0; times <= pageId % 4; times++) {
                Touch(cache, *pages[pageId]);
            }
        }
        Touch(cache, *pages[14]);
        Touch(cache, *pages[15]);
        Touch(cache, *pages[15]);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {29 1f 1b}" << Endl
            << "MainQueue: {11 0f 1b}, {14 1f 1b}, {15 2f 1b}, {18 0f 1b}, {19 0f 1b}, {22 0f 1b}, {23 0f 1b}, {26 0f 1b}, {27 0f 1b}" << Endl
            << "GhostQueue: 9, 12, 13, 16, 17, 20, 21, 24, 25, 28"));

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{29});
        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{11});
        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{18});
        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{19});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {22 0f 1b}, {23 0f 1b}, {26 0f 1b}, {27 0f 1b}, {14 0f 1b}, {15 1f 1b}" << Endl
            << "GhostQueue: 13, 16, 17, 20, 21, 24, 25, 28, 29"));
    }

    Y_UNIT_TEST(UpdateLimit) {
        TS3FIFOCache<TPage, TPageTraits> cache(10);

        TVector<THolder<TPage>> pages;
        for (ui32 pageId : xrange(30)) {
            pages.push_back(MakeHolder<TPage>(pageId, 1));
        }

        for (ui32 pageId : xrange(30)) {
            for (ui32 times = 0; times <= pageId % 4; times++) {
                Touch(cache, *pages[pageId]);
            }
        }
        Touch(cache, *pages[14]);
        Touch(cache, *pages[15]);
        Touch(cache, *pages[15]);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {29 1f 1b}" << Endl
            << "MainQueue: {11 0f 1b}, {14 1f 1b}, {15 2f 1b}, {18 0f 1b}, {19 0f 1b}, {22 0f 1b}, {23 0f 1b}, {26 0f 1b}, {27 0f 1b}" << Endl
            << "GhostQueue: 9, 12, 13, 16, 17, 20, 21, 24, 25, 28"));
        
        cache.UpdateLimit(6);
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, *pages[0]), (TVector<ui32>{29, 0, 11, 18, 19}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {22 0f 1b}, {23 0f 1b}, {26 0f 1b}, {27 0f 1b}, {14 0f 1b}, {15 1f 1b}" << Endl
            << "GhostQueue: 16, 17, 20, 21, 24, 25, 28, 29, 0"));
    }

    Y_UNIT_TEST(Erase) {
        TS3FIFOCache<TPage, TPageTraits> cache(10);

        TVector<THolder<TPage>> pages;
        for (ui32 pageId : xrange(30)) {
            pages.push_back(MakeHolder<TPage>(pageId, 1));
        }

        for (ui32 pageId : xrange(30)) {
            for (ui32 times = 0; times <= pageId % 4; times++) {
                Touch(cache, *pages[pageId]);
            }
        }
        Touch(cache, *pages[14]);
        Touch(cache, *pages[15]);
        Touch(cache, *pages[15]);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {29 1f 1b}" << Endl
            << "MainQueue: {11 0f 1b}, {14 1f 1b}, {15 2f 1b}, {18 0f 1b}, {19 0f 1b}, {22 0f 1b}, {23 0f 1b}, {26 0f 1b}, {27 0f 1b}" << Endl
            << "GhostQueue: 9, 12, 13, 16, 17, 20, 21, 24, 25, 28"));
        
        Erase(cache, *pages[29]);
        Erase(cache, *pages[22]);
        Erase(cache, *pages[26]);
        Erase(cache, *pages[17]);
        Cerr << cache.Dump() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {11 0f 1b}, {14 1f 1b}, {15 2f 1b}, {18 0f 1b}, {19 0f 1b}, {23 0f 1b}, {27 0f 1b}" << Endl
            << "GhostQueue: 9, 12, 13, 16, 17, 20, 21, 24, 25, 28"));
    }

    Y_UNIT_TEST(Random) {
        TS3FIFOCache<TPage, TPageTraits> cache(100);

        TVector<THolder<TPage>> pages;
        for (ui32 pageId : xrange(500)) {
            pages.push_back(MakeHolder<TPage>(pageId, 1));
        }

        ui32 hits = 0, misses = 0;

        for (ui32 i = 0; i < 100000; i++) {
            ui32 pageId = std::sqrt(RandomNumber<ui32>(pages.size() * pages.size()));
            TPage* page = pages[pageId].Get();
            if (TPageTraits::GetLocation(page) != ES3FIFOPageLocation::None) {
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
