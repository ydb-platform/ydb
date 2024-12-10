#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tablet_flat/shared_cache_switchable.h>
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

            while (GetSize() > Limit) {
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
                Map.erase(page->Id);
            }
        }

        void UpdateLimit(ui64 limit) override {
            Limit = limit;
        }

        ui64 GetSize() const override {
            ui64 size = 0;
            for (auto page : List) {
                size += page->Size;
            }
            return size;
        }

        TString Dump() const override {
            TStringBuilder result;
            size_t count = 0;
            for (auto it = List.begin(); it != List.end(); it++) {
                TPage* page = *it;
                if (count != 0) result << ", ";
                result << "{" << page->Id << " " << page->Size << "b}";
                count++;
                Y_ABORT_UNLESS(*Map.FindPtr(page->Id) == it);
            }
            Y_ABORT_UNLESS(Map.size() == count);
            return result;
        }
    
    private:
        ui64 Limit = 0;
        TList<TPage*> List;
        THashMap<ui32, TList<TPage*>::iterator> Map;
    };

}

Y_UNIT_TEST_SUITE(TSwitchableCache) {

    TVector<ui32> Touch(auto& cache, TPage& page) {
        auto evicted = cache.Touch(&page);
        TVector<ui32> result;
        for (auto& p : evicted) {
            UNIT_ASSERT_VALUES_EQUAL(p.CacheId, 0);
            result.push_back(p.Id);
        }
        return result;
    }

    void Erase(auto& cache, TPage& page) {
        cache.Erase(&page);
        UNIT_ASSERT_VALUES_EQUAL(page.CacheId, 0);
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
        TCounterPtr counter = new NMonitoring::TCounterForPtr;
        TSwitchableCache<TPage, TPageTraits> cache(10, MakeHolder<TSimpleCache>(), counter);

        TPage page1{1, 2};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{1 2b}");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());
        
        TPage page2{2, 3};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{1 2b}, {2 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());

        TPage page3{3, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{1 2b}, {2 3b}, {3 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());

        TPage page4{4, 1};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{1 2b}, {2 3b}, {3 4b}, {4 1b}");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());

        TPage page5{5, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), (TVector<ui32>{1, 2}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{3 4b}, {4 1b}, {5 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), (TVector<ui32>{3}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{4 1b}, {5 4b}, {2 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());

        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page5), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{4 1b}, {2 3b}, {5 4b}");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());
    }

    Y_UNIT_TEST(Erase) {
        TCounterPtr counter = new NMonitoring::TCounterForPtr;
        TSwitchableCache<TPage, TPageTraits> cache(10, MakeHolder<TSimpleCache>(), counter);

        TPage page1{1, 2};
        TPage page2{2, 3};
        TPage page3{3, 4};
        TPage page4{4, 1};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{1 2b}, {2 3b}, {3 4b}, {4 1b}");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());
        
        Erase(cache, page2);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{1 2b}, {3 4b}, {4 1b}");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());

        TPage page5{5, 4};
        Erase(cache, page5);
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{1 2b}, {3 4b}, {4 1b}");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());
    }

    Y_UNIT_TEST(EvictNext) {
        TCounterPtr counter = new NMonitoring::TCounterForPtr;
        TSwitchableCache<TPage, TPageTraits> cache(10, MakeHolder<TSimpleCache>(), counter);

        TPage page1{1, 2};
        TPage page2{2, 3};
        TPage page3{3, 4};
        TPage page4{4, 1};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{1 2b}, {2 3b}, {3 4b}, {4 1b}");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());
        
        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), (TVector<ui32>{1}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{2 3b}, {3 4b}, {4 1b}");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), (TVector<ui32>{2}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{3 4b}, {4 1b}");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), (TVector<ui32>{3}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{4 1b}");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), (TVector<ui32>{4}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), (TVector<ui32>{}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());
    }

    Y_UNIT_TEST(UpdateLimit) {
        TCounterPtr counter = new NMonitoring::TCounterForPtr;
        TSwitchableCache<TPage, TPageTraits> cache(10, MakeHolder<TSimpleCache>(), counter);

        TPage page1{1, 2};
        TPage page2{2, 3};
        TPage page3{3, 4};
        TPage page4{4, 1};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{1 2b}, {2 3b}, {3 4b}, {4 1b}");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());
        
        cache.UpdateLimit(6);
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), (TVector<ui32>{1, 3}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{4 1b}, {2 3b}");
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), cache.GetSize());
    }

    Y_UNIT_TEST(Switch_Touch_RotatePages_All) {
        TCounterPtr counter1 = new NMonitoring::TCounterForPtr;
        TCounterPtr counter2 = new NMonitoring::TCounterForPtr;
        TSwitchableCache<TPage, TPageTraits> cache(10, MakeHolder<TSimpleCache>(), counter1);

        TPage page1{1, 2};
        TPage page2{2, 3};
        TPage page3{3, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{1 2b}, {2 3b}, {3 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 9);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 9);
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 0);

        UNIT_ASSERT_VALUES_EQUAL(Switch(cache, MakeHolder<TSimpleCache>(), counter2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{1 2b}, {2 3b}, {3 4b}; ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 9);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 9);
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 0);

        TPage page4{4, 1};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page4), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{4 1b}, {1 2b}, {2 3b}, {3 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 10);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 10);
    }

    Y_UNIT_TEST(Switch_Touch_RotatePages_Parts) {
        TCounterPtr counter1 = new NMonitoring::TCounterForPtr;
        TCounterPtr counter2 = new NMonitoring::TCounterForPtr;
        TSwitchableCache<TPage, TPageTraits> cache(50, MakeHolder<TSimpleCache>(), counter1);

        TVector<THolder<TPage>> pages;
        for (ui32 pageId : xrange(50)) {
            pages.push_back(MakeHolder<TPage>(pageId, 1));
            UNIT_ASSERT_VALUES_EQUAL(Touch(cache, *pages.back()), TVector<ui32>{});
        }

        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 50);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 50);
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 0);

        UNIT_ASSERT_VALUES_EQUAL(Switch(cache, MakeHolder<TSimpleCache>(), counter2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 50);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 50);
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 0);

        pages.push_back(MakeHolder<TPage>(pages.size(), 1));
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, *pages.back()), TVector<ui32>{10});
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 50);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 39); // [11 .. 49]
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 11); // [50, 0 .. 9]

        pages.push_back(MakeHolder<TPage>(pages.size(), 1));
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, *pages.back()), TVector<ui32>{21});
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 50);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 28);
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 22);

        pages.push_back(MakeHolder<TPage>(pages.size(), 1));
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, *pages.back()), TVector<ui32>{32});
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 50);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 17);
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 33);

        pages.push_back(MakeHolder<TPage>(pages.size(), 1));
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, *pages.back()), TVector<ui32>{43});
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 50);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 6);
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 44);

        pages.push_back(MakeHolder<TPage>(pages.size(), 1));
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, *pages.back()), TVector<ui32>{50});
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 50);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 50);
    }

    Y_UNIT_TEST(Switch_RotatePages_Force) {
        TCounterPtr counter = new NMonitoring::TCounterForPtr;
        TSwitchableCache<TPage, TPageTraits> cache(10, MakeHolder<TSimpleCache>(), counter);

        TPage page1{1, 2};
        TPage page2{2, 3};
        TPage page3{3, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{1 2b}, {2 3b}, {3 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 9);
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), 9);

        UNIT_ASSERT_VALUES_EQUAL(Switch(cache, MakeHolder<TSimpleCache>(), counter), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{1 2b}, {2 3b}, {3 4b}; ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 9);
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), 9);

        UNIT_ASSERT_VALUES_EQUAL(Switch(cache, MakeHolder<TSimpleCache>(), counter), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{1 2b}, {2 3b}, {3 4b}; ; ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 9);
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), 9);

        UNIT_ASSERT_VALUES_EQUAL(Switch(cache, MakeHolder<TSimpleCache>(), counter), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{1 2b}, {2 3b}, {3 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 9);
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), 9);
    }

    Y_UNIT_TEST(Switch_RotatePages_Evicts) {
        TCounterPtr counter = new NMonitoring::TCounterForPtr;
        TSwitchableCache<TPage, TPageTraits> cache(10, MakeHolder<TSimpleCache>(), counter);

        TPage page1{1, 2};
        TPage page2{2, 3};
        TPage page3{3, 4};
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page1), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page2), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, page3), TVector<ui32>{});

        UNIT_ASSERT_VALUES_EQUAL(Switch(cache, MakeHolder<TSimpleCache>(), counter), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(Switch(cache, MakeHolder<TSimpleCache>(), counter), TVector<ui32>{});
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{1 2b}, {2 3b}, {3 4b}; ; ");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 9);
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), 9);

        cache.UpdateLimit(5);

        UNIT_ASSERT_VALUES_EQUAL(Switch(cache, MakeHolder<TSimpleCache>(), counter), (TVector<ui32>{1, 2}));
        UNIT_ASSERT_VALUES_EQUAL(cache.Dump(), "{3 4b}");
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 4);
        UNIT_ASSERT_VALUES_EQUAL(counter->Val(), 4);
    }

    Y_UNIT_TEST(Switch_Touch) {
        TCounterPtr counter1 = new NMonitoring::TCounterForPtr;
        TCounterPtr counter2 = new NMonitoring::TCounterForPtr;
        TSwitchableCache<TPage, TPageTraits> cache(50, MakeHolder<TSimpleCache>(), counter1);

        TVector<THolder<TPage>> pages;
        for (ui32 pageId : xrange(50)) {
            pages.push_back(MakeHolder<TPage>(pageId, 1));
            UNIT_ASSERT_VALUES_EQUAL(Touch(cache, *pages.back()), TVector<ui32>{});
        }

        UNIT_ASSERT_VALUES_EQUAL(Switch(cache, MakeHolder<TSimpleCache>(), counter2), TVector<ui32>{});

        pages.push_back(MakeHolder<TPage>(pages.size(), 1));
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, *pages.back()), TVector<ui32>{10});
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 50);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 39); // [11 .. 49]
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 11); // [50, 0 .. 9]

        Touch(cache, *pages[23]);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 50);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 28);
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 22);

        Touch(cache, *pages[7]);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 50);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 18);
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 32);
    }

    Y_UNIT_TEST(Switch_Erase) {
        TCounterPtr counter1 = new NMonitoring::TCounterForPtr;
        TCounterPtr counter2 = new NMonitoring::TCounterForPtr;
        TSwitchableCache<TPage, TPageTraits> cache(50, MakeHolder<TSimpleCache>(), counter1);

        TVector<THolder<TPage>> pages;
        for (ui32 pageId : xrange(50)) {
            pages.push_back(MakeHolder<TPage>(pageId, 1));
            UNIT_ASSERT_VALUES_EQUAL(Touch(cache, *pages.back()), TVector<ui32>{});
        }

        UNIT_ASSERT_VALUES_EQUAL(Switch(cache, MakeHolder<TSimpleCache>(), counter2), TVector<ui32>{});

        pages.push_back(MakeHolder<TPage>(pages.size(), 1));
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, *pages.back()), TVector<ui32>{10});
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 50);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 39); // [11 .. 49]
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 11); // [50, 0 .. 9]

        Erase(cache, *pages[23]);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 49);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 38);
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 11);

        Erase(cache, *pages[7]);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 48);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 38);
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 10);
    }

    Y_UNIT_TEST(Switch_EvictNext) {
        TCounterPtr counter1 = new NMonitoring::TCounterForPtr;
        TCounterPtr counter2 = new NMonitoring::TCounterForPtr;
        TSwitchableCache<TPage, TPageTraits> cache(50, MakeHolder<TSimpleCache>(), counter1);

        TVector<THolder<TPage>> pages;
        for (ui32 pageId : xrange(50)) {
            pages.push_back(MakeHolder<TPage>(pageId, 1));
            UNIT_ASSERT_VALUES_EQUAL(Touch(cache, *pages.back()), TVector<ui32>{});
        }

        UNIT_ASSERT_VALUES_EQUAL(Switch(cache, MakeHolder<TSimpleCache>(), counter2), TVector<ui32>{});

        pages.push_back(MakeHolder<TPage>(pages.size(), 1));
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, *pages.back()), TVector<ui32>{10});
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 50);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 39); // [11 .. 49]
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 11); // [50, 0 .. 9]

        for (ui32 i : xrange(39)) {
            UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{i + 11});
        }
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 11);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 11);

        UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{50});
        for (ui32 i : xrange(10)) {
            UNIT_ASSERT_VALUES_EQUAL(EvictNext(cache), TVector<ui32>{i});
        }
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 0);
    }

    Y_UNIT_TEST(Switch_UpdateLimit) {
        TCounterPtr counter1 = new NMonitoring::TCounterForPtr;
        TCounterPtr counter2 = new NMonitoring::TCounterForPtr;
        TSwitchableCache<TPage, TPageTraits> cache(50, MakeHolder<TSimpleCache>(), counter1);

        TVector<THolder<TPage>> pages;
        for (ui32 pageId : xrange(50)) {
            pages.push_back(MakeHolder<TPage>(pageId, 1));
            UNIT_ASSERT_VALUES_EQUAL(Touch(cache, *pages.back()), TVector<ui32>{});
        }

        UNIT_ASSERT_VALUES_EQUAL(Switch(cache, MakeHolder<TSimpleCache>(), counter2), TVector<ui32>{});

        pages.push_back(MakeHolder<TPage>(pages.size(), 1));
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, *pages.back()), TVector<ui32>{10});
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 50);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 39); // [11 .. 49]
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 11); // [50, 0 .. 9]

        cache.UpdateLimit(40);
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, *pages[23]), (TVector<ui32>{21, 22, 24, 25, 26, 27, 28, 29, 30, 31}));
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 40);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 18); // [32 .. 49]
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 22); // [50, 0 .. 9, 23, 11 .. 20]

        cache.UpdateLimit(7);
        UNIT_ASSERT_VALUES_EQUAL(Touch(cache, *pages[7]).size(), 33);
        UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 7);
        UNIT_ASSERT_VALUES_EQUAL(counter1->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counter2->Val(), 7);
    }

}

}
