#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tablet_flat/shared_cache_composite.h>
#include <ydb/core/util/cache_cache_iface.h>

namespace NKikimr::NCache {

namespace {

    using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

    struct TPage : public TIntrusiveListItem<TPage> {
        ui32 Id;
        size_t Size;

        TPage(ui32 id, size_t size) 
            : Id(id), Size(size)
        {}

        ui32 CacheId : 4 = 0;
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

        static ui32 GetCacheId(const TPage* page) {
            return page->CacheId;
        }

        static void SetCacheId(TPage* page, ui32 id) {
            Y_ABORT_UNLESS(id < (1 << 4));
            page->CacheId = id;
        }
    };

    class TSimpleCache : public ICacheCache<TPage> {
    public:
        TSimpleCache(ui64 limit)
            : Limit(limit)
        {
        }

        TIntrusiveList<TPage> EvictNext() override {
            TIntrusiveList<TPage> result;
            
            if (!List.empty()) {
                TPage* page = List.front();
                List.pop_front();
                Map.erase(page->Id);
                result.PushBack(page);
            };

            return result;
        }

        TIntrusiveList<TPage> Touch(TPage* page) override {
            if (Map.contains(page->Id)) {
                List.erase(Map[page->Id]);
            }
            List.push_back(page);
            Map[page->Id] = prev(List.end());

            TIntrusiveList<TPage> evictedList;

            while (Size > Limit) {
                TPage* page = List.front();
                List.pop_front();
                Map.erase(page->Id);
                evictedList.PushBack(page);
            }

            return evictedList;
        }

        void Erase(TPage* page) override {
            if (Map.contains(page->Id)) {
                List.erase(Map[page->Id]);
                Size -= page->Size;
                Map.erase(page->Id);
            }
        }

        void UpdateLimit(ui64 limit) override {
            Limit = limit;
        }

        ui64 GetSize() const override {
            return Size;
        }

        TString Dump() const override {
            TStringBuilder result;
            size_t count = 0;
            ui64 size = 0;
            for (auto it = List.begin(); it != List.end(); it++) {
                TPage* page = *it;
                if (count != 0) result << ", ";
                result << page->Id;
                count++;
                size += page->Size;
                Y_ABORT_UNLESS(*Map.FindPtr(page->Id) == it);
            }
            Y_ABORT_UNLESS(Size == size);
            Y_ABORT_UNLESS(Map.size() == count);
            return result;
        }
    
    private:
        ui64 Limit;
        ui64 Size = 0;
        TList<TPage*> List;
        THashMap<ui32, TList<TPage*>::iterator> Map;
    };

}

Y_UNIT_TEST_SUITE(TCompositeCache) {

    TVector<ui32> Touch(auto& cache, TPage& page) {
        auto evicted = cache.Touch(&page);
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheId, 0);
            result.push_back(p.Id);
        }
        return result;
    }

    TVector<ui32> EvictNext(auto& cache) {
        auto evicted = cache.EvictNext();
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheId, 0);
            result.push_back(p.Id);
        }
        return result;
    }

    Y_UNIT_TEST(One_Touch) {
        TCounterPtr counter = new NMonitoring::TCounterForPtr;
        TCompositeCache<TPage, TPageTraits> cache(MakeHolder<TSimpleCache>(100), counter);

        // TPage page1{1, 2};
        // UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        // UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
        //     << "SmallQueue: {1 0f 2b}" << Endl
        //     << "MainQueue: " << Endl
        //     << "GhostQueue: "));
        
        // TPage page2{2, 3};
        // UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        // UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
        //     << "SmallQueue: {1 0f 2b}, {2 0f 3b}" << Endl
        //     << "MainQueue: " << Endl
        //     << "GhostQueue: "));
        
        // TPage page3{3, 4};
        // UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        // UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
        //     << "SmallQueue: {1 0f 2b}, {2 0f 3b}, {3 0f 4b}" << Endl
        //     << "MainQueue: " << Endl
        //     << "GhostQueue: "));

        // TPage page4{4, 1};
        // UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        // UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
        //     << "SmallQueue: {1 0f 2b}, {2 0f 3b}, {3 0f 4b}, {4 0f 1b}" << Endl
        //     << "MainQueue: " << Endl
        //     << "GhostQueue: "));
        
        // UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        // UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        // UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        // UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        // UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        // UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
        //     << "SmallQueue: {1 2f 2b}, {2 1f 3b}, {3 2f 4b}, {4 0f 1b}" << Endl
        //     << "MainQueue: " << Endl
        //     << "GhostQueue: "));

        // TPage page5{5, 8};
        // UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), TVector<ui32>{2});
        // UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
        //     << "SmallQueue: {4 0f 1b}, {5 0f 8b}" << Endl
        //     << "MainQueue: {1 2f 2b}, {3 2f 4b}" << Endl
        //     << "GhostQueue: {2 3b}"));

        // UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        // UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        // UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        // UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        // UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
        //     << "SmallQueue: {4 0f 1b}, {5 0f 8b}" << Endl
        //     << "MainQueue: {1 2f 2b}, {3 3f 4b}" << Endl
        //     << "GhostQueue: {2 3b}"));
        
        // UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        // UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), (TString)(TStringBuilder()
        //     << "SmallQueue: {4 0f 1b}, {5 0f 8b}" << Endl
        //     << "MainQueue: {1 2f 2b}, {3 3f 4b}, {2 0f 3b}" << Endl
        //     << "GhostQueue: "));
    }
}

}
