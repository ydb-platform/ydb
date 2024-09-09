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

    struct TPageKey {
        ui32 Id;

        TPageKey(ui32 id)
            : Id(id)
        {}

        static TPageKey Get(const TPage* page) {
            return {page->Id};
        }

        TString ToString() const {
            return std::to_string(Id);
        }
    };

    struct TPageKeyHash {
        inline size_t operator()(const TPageKey& key) const {
            return std::hash<ui32>()(key.Id);
        }
    };

    struct TPageKeyEqual {
        inline bool operator()(const TPageKey& left, const TPageKey& right) const {
            return left.Id == right.Id;
        }
    };

    struct TPageSize {
        static ui64 Get(const TPage *page) {
            return page->Size;
        }
    };

    struct TPageLocation {
        static ui32 Get(const TPage *page) {
            return page->CacheFlags1;
        }
        static void Set(TPage *x, ui32 flags) {
            Y_ABORT_UNLESS(flags < (1 << 4));
            x->CacheFlags1 = flags;
        }
    };

    struct TPageFrequency {
        static ui32 Get(const TPage *page) {
            return page->CacheFlags2;
        }
        static void Set(TPage *page, ui32 flags) {
            Y_ABORT_UNLESS(flags < (1 << 4));
            page->CacheFlags2 = flags;
        }
    };

}

Y_UNIT_TEST_SUITE(TTS3FIFOGhostQueue) {
    
    Y_UNIT_TEST(Add) {
        TTS3FIFOGhostQueue<TPageKey, TPageKeyHash, TPageKeyEqual> queue(100);
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
        TTS3FIFOGhostQueue<TPageKey, TPageKeyHash, TPageKeyEqual> queue(100);
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
        TTS3FIFOGhostQueue<TPageKey, TPageKeyHash, TPageKeyEqual> queue(100);
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
        TTS3FIFOGhostQueue<TPageKey, TPageKeyHash, TPageKeyEqual> queue(100);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "");

        queue.Add(1, 101);
        UNIT_ASSERT_VALUES_EQUAL(queue.Dump(), "");
    }

    Y_UNIT_TEST(UpdateLimit) {
        TTS3FIFOGhostQueue<TPageKey, TPageKeyHash, TPageKeyEqual> queue(100);
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

    TVector<TPage*> Touch(auto& cache, TPage& page) {
        auto evicted = cache.Touch(&page);
        TVector<TPage*> result;
        for (auto& p : evicted) {
            result.push_back(&p);
        }
        return result;
    }

    Y_UNIT_TEST(Touch) {
        TS3FIFOCache<TPage, TPageKey, TPageKeyHash, TPageKeyEqual, TPageSize, TPageLocation, TPageFrequency> cache(100);

        TPage page1{1, 2};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<TPage*>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {1 0f 2b}" << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: "));
        
        TPage page2{2, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<TPage*>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {1 0f 2b}, {2 0f 3b}" << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: "));
        
        TPage page3{3, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<TPage*>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {1 0f 2b}, {2 0f 3b}, {3 0f 4b}" << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: "));

        TPage page4{4, 1};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<TPage*>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {1 0f 2b}, {2 0f 3b}, {3 0f 4b}, {4 0f 1b}" << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: "));

        
    }

}

}
