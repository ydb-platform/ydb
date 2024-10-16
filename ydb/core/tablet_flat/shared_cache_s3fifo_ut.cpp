#include <library/cpp/testing/unittest/registar.h>
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
    
    Y_UNIT_TEST(Add) {
        TS3FIFOGhostPageQueue<TPageTraits> queue(100);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "");

        queue.Add(1, 10);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}");

        queue.Add(2, 30);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {2 30b}");

        queue.Add(3, 60);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {2 30b}, {3 60b}");

        queue.Add(4, 1);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{2 30b}, {3 60b}, {4 1b}");

        queue.Add(1, 3);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{2 30b}, {3 60b}, {4 1b}, {1 3b}");
    }

    Y_UNIT_TEST(Erase) {
        TS3FIFOGhostPageQueue<TPageTraits> queue(100);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "");

        queue.Add(1, 10);
        queue.Add(2, 30);
        queue.Add(3, 60);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {2 30b}, {3 60b}");

        UNIT_ASSERT_VALUES_EQUAL(queue.Erase(5, 42), false);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {2 30b}, {3 60b}");

        UNIT_ASSERT_VALUES_EQUAL(queue.Erase(2, 30), true);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {3 60b}");

        queue.Add(4, 30);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {3 60b}, {4 30b}");

        queue.Add(5, 1);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{3 60b}, {4 30b}, {5 1b}");
    }

    Y_UNIT_TEST(Erase_Add) {
        TS3FIFOGhostPageQueue<TPageTraits> queue(100);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "");

        queue.Add(1, 10);
        queue.Add(2, 30);
        queue.Add(3, 60);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {2 30b}, {3 60b}");

        UNIT_ASSERT_VALUES_EQUAL(queue.Erase(2, 30), true);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {3 60b}");

        queue.Add(2, 30);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {3 60b}, {2 30b}");

        queue.Add(4, 70);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{2 30b}, {4 70b}");
    }

    Y_UNIT_TEST(Add_Big) {
        TS3FIFOGhostPageQueue<TPageTraits> queue(100);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "");

        queue.Add(1, 101);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "");
    }

    Y_UNIT_TEST(UpdateLimit) {
        TS3FIFOGhostPageQueue<TPageTraits> queue(100);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "");

        queue.Add(1, 10);
        queue.Add(2, 20);
        queue.Add(3, 30);
        queue.Add(4, 40);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{1 10b}, {2 20b}, {3 30b}, {4 40b}");

        queue.UpdateLimit(80);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "{3 30b}, {4 40b}");
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

    Y_UNIT_TEST(Touch) {
        TS3FIFOCache<TPage, TPageTraits> cache(100);

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
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {1 2f 2b}, {2 1f 3b}, {3 2f 4b}, {4 0f 1b}" << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: "));

        TPage page5{5, 8};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), TVector<ui32>{2});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {4 0f 1b}, {5 0f 8b}" << Endl
            << "MainQueue: {1 2f 2b}, {3 2f 4b}" << Endl
            << "GhostQueue: {2 3b}"));

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {4 0f 1b}, {5 0f 8b}" << Endl
            << "MainQueue: {1 2f 2b}, {3 3f 4b}" << Endl
            << "GhostQueue: {2 3b}"));
        
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {4 0f 1b}, {5 0f 8b}" << Endl
            << "MainQueue: {1 2f 2b}, {3 3f 4b}, {2 0f 3b}" << Endl
            << "GhostQueue: "));
    }

    Y_UNIT_TEST(Touch_MainQueue) {
        TS3FIFOCache<TPage, TPageTraits> cache(100);

        TPage page1{1, 20};
        TPage page2{2, 30};
        TPage page3{3, 40};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{1});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{2});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{3});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: {1 20b}, {2 30b}, {3 40b}"));

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {2 0f 30b}, {1 0f 20b}, {3 0f 40b}" << Endl
            << "GhostQueue: "));
        
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {2 2f 30b}, {1 1f 20b}, {3 0f 40b}" << Endl
            << "GhostQueue: "));
        
        TPage page4{4, 20};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{4});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {2 2f 30b}, {1 1f 20b}, {3 0f 40b}" << Endl
            << "GhostQueue: {4 20b}"));
        
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{3});
        // MainQueue: {2 2f 30b}, {1 1f 20b}, {3 0f 40b}, {4 0f 20b}
        // MainQueue: {1 1f 20b}, {3 0f 40b}, {4 0f 20b}, {2 1f 30b}
        // MainQueue: {3 0f 40b}, {4 0f 20b}, {2 1f 30b}, {1 0f 20b}
        // MainQueue: {4 0f 20b}, {2 1f 30b}, {1 0f 20b}
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {4 0f 20b}, {2 1f 30b}, {1 0f 20b}" << Endl
            << "GhostQueue: "));
    }

    Y_UNIT_TEST(EvictNext) {
        TS3FIFOCache<TPage, TPageTraits> cache(100);

        TPage page1{1, 20};
        TPage page2{2, 30};
        TPage page3{3, 40};
        TPage page4{4, 10};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{1});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{2});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{3});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: {1 20b}, {2 30b}, {3 40b}"));

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {2 0f 30b}, {1 1f 20b}, {3 0f 40b}" << Endl
            << "GhostQueue: "));
        
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {4 0f 10b}" << Endl
            << "MainQueue: {2 0f 30b}, {1 1f 20b}, {3 0f 40b}" << Endl
            << "GhostQueue: "));
        
        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{4});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {2 0f 30b}, {1 1f 20b}, {3 0f 40b}" << Endl
            << "GhostQueue: {4 10b}"));
        
        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{2});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {1 1f 20b}, {3 0f 40b}" << Endl
            << "GhostQueue: {4 10b}"));
        
        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{3});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {1 0f 20b}" << Endl
            << "GhostQueue: {4 10b}"));

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{1});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: {4 10b}"));

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: {4 10b}"));
    }

    Y_UNIT_TEST(UpdateLimit) {
        TS3FIFOCache<TPage, TPageTraits> cache(100);

        TPage page1{1, 20};
        TPage page2{2, 30};
        TPage page3{3, 40};
        TPage page4{4, 10};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{1});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{2});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{3});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: {1 20b}, {2 30b}, {3 40b}"));

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {2 0f 30b}, {1 0f 20b}, {3 0f 40b}" << Endl
            << "GhostQueue: "));
        
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {4 0f 10b}" << Endl
            << "MainQueue: {2 0f 30b}, {1 0f 20b}, {3 0f 40b}" << Endl
            << "GhostQueue: "));
        
        cache.UpdateLimit(45);
        TPage page5{5, 1};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), (TVector<ui32>{4, 2, 1}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {5 0f 1b}" << Endl
            << "MainQueue: {3 0f 40b}" << Endl
            << "GhostQueue: {4 10b}"));
        
        cache.UpdateLimit(0);
        TPage page6{6, 1};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page6), (TVector<ui32>{5, 6, 3}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: "));
    }

    Y_UNIT_TEST(Erase) {
        TS3FIFOCache<TPage, TPageTraits> cache(100);

        TPage page1{1, 20};
        TPage page2{2, 30};
        TPage page3{3, 40};
        TPage page4{4, 10};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{1});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{2});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{3});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: {1 20b}, {2 30b}, {3 40b}"));
        
        // Erase from Ghost Queue:
        cache.Erase(&page2);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: {1 20b}, {3 40b}"));
        
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{2});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: {1 20b}, {3 40b}, {2 30b}"));

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {1 0f 20b}, {2 0f 30b}, {3 0f 40b}" << Endl
            << "GhostQueue: "));
        
        // Erase from Main Queue:
        cache.Erase(&page2);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {1 0f 20b}, {3 0f 40b}" << Endl
            << "GhostQueue: "));

        page2.Size = 6;  
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {2 0f 6b}" << Endl
            << "MainQueue: {1 0f 20b}, {3 0f 40b}" << Endl
            << "GhostQueue: "));
        
        // Erase non-existing:
        TPage page42{42, 1};
        cache.Erase(&page42);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {2 0f 6b}" << Endl
            << "MainQueue: {1 0f 20b}, {3 0f 40b}" << Endl
            << "GhostQueue: "));

        // Erase from Small Queue:
        cache.Erase(&page2);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {1 0f 20b}, {3 0f 40b}" << Endl
            << "GhostQueue: "));
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
