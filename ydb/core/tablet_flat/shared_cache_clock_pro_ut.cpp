#include <library/cpp/testing/unittest/registar.h>
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

    std::optional<ui32> EvictNext(auto& cache) {
        auto evicted = cache.EvictNext();
        if (evicted) {
            UNIT_ASSERT_VALUES_EQUAL(evicted->CacheFlags1, 0);
            UNIT_ASSERT_VALUES_EQUAL(evicted->CacheFlags2, 0);
            return evicted->Id;
        }
        return {};
    }

    Y_UNIT_TEST(Touch) {
        TClockProCache<TPage, TPageTraits> cache(10);

        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "");

        TPage page1{1, 2};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Cold>Test>{1 C 0r 2b}");
        
        TPage page2{2, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{1 C 0r 2b}, Cold>{2 C 0r 3b}");

        TPage page3{3, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{1 C 0r 2b}, Cold>{2 C 0r 3b}, {3 C 0r 4b}");

        TPage page4{4, 1};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{1 C 0r 2b}, Cold>{2 C 0r 3b}, {3 C 0r 4b}, {4 C 0r 1b}");
        
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{1 C 1r 2b}, Cold>{2 C 0r 3b}, {3 C 1r 4b}, {4 C 0r 1b}");

        TPage page5{5, 1};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "Hot>Test>{1 C 1r 2b}, Cold>{2 C 0r 3b}, {3 C 1r 4b}, {4 C 0r 1b}, {5 C 0r 1b}");
    }

    Y_UNIT_TEST(EvictNext) {
        TClockProCache<TPage, TPageTraits> cache(100);

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
            << "MainQueue: {2 0r 30b}, {1 1r 20b}, {3 0r 40b}" << Endl
            << "GhostQueue: "));
        
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {4 0r 10b}" << Endl
            << "MainQueue: {2 0r 30b}, {1 1r 20b}, {3 0r 40b}" << Endl
            << "GhostQueue: "));
        
        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), 4);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {2 0r 30b}, {1 1r 20b}, {3 0r 40b}" << Endl
            << "GhostQueue: {4 10b}"));
        
        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), 2);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {1 1r 20b}, {3 0r 40b}" << Endl
            << "GhostQueue: {4 10b}"));
        
        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), 3);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: {1 0r 20b}" << Endl
            << "GhostQueue: {4 10b}"));

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), 1);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: {4 10b}"));

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), std::optional<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: " << Endl
            << "MainQueue: " << Endl
            << "GhostQueue: {4 10b}"));
    }

    Y_UNIT_TEST(UpdateLimit) {
        TClockProCache<TPage, TPageTraits> cache(100);

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
            << "MainQueue: {2 0r 30b}, {1 0r 20b}, {3 0r 40b}" << Endl
            << "GhostQueue: "));
        
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {4 0r 10b}" << Endl
            << "MainQueue: {2 0r 30b}, {1 0r 20b}, {3 0r 40b}" << Endl
            << "GhostQueue: "));
        
        cache.UpdateLimit(45);
        TPage page5{5, 1};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), (TVector<ui32>{4, 2, 1}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
            << "SmallQueue: {5 0r 1b}" << Endl
            << "MainQueue: {3 0r 40b}" << Endl
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
        TClockProCache<TPage, TPageTraits> cache(100);

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
    }

}

}
