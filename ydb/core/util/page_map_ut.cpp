#include "page_map.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>
#include <util/random/shuffle.h>

#include <memory>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TPageMapTest) {
    struct TPage {
        ui32 PageId;

        explicit TPage(ui32 pageId)
            : PageId(pageId)
        { }
    };

    Y_UNIT_TEST(TestResize) {
        TPageMap<THolder<TPage>> map;
        UNIT_ASSERT_VALUES_EQUAL(map.size(), 0u);

        map.resize(1024);
        UNIT_ASSERT_VALUES_EQUAL(map.size(), 1024u);
        UNIT_ASSERT_VALUES_EQUAL(map.used(), 0u);

        UNIT_ASSERT(map.emplace(0, MakeHolder<TPage>(0)));
        UNIT_ASSERT(map.emplace(1, MakeHolder<TPage>(1)));
        UNIT_ASSERT_VALUES_EQUAL(map.used(), 2u);

        for (ui32 id = 0; id < 2; ++id) {
            UNIT_ASSERT(map[id]);
            UNIT_ASSERT_VALUES_EQUAL(map[id]->PageId, id);
        }
        for(ui32 id = 2; id < 1024; ++id) {
            UNIT_ASSERT(!map[id]);
            UNIT_ASSERT(map.emplace(id, MakeHolder<TPage>(id)));
        }

        map.resize(1024);
        map.resize(2048);
        UNIT_ASSERT_VALUES_EQUAL(map.size(), 2048u);
        UNIT_ASSERT_VALUES_EQUAL(map.used(), 1024u);

        for (ui32 id = 0; id < 2048; ++id) {
            if (id < 1024) {
                UNIT_ASSERT(map[id]);
                UNIT_ASSERT_VALUES_EQUAL(map[id]->PageId, id);
            } else {
                UNIT_ASSERT(!map[id]);
            }
        }

        UNIT_ASSERT(map.erase(0));
        UNIT_ASSERT(!map[0]);
        UNIT_ASSERT_VALUES_EQUAL(map.used(), 1023u);
    }

    Y_UNIT_TEST(TestRandom) {
        TPageMap<THolder<TPage>> map;
        map.resize(1024 * 1024);

        TVector<ui32> pageIds(Reserve(map.size()));
        for (ui32 pageId = 0; pageId < map.size(); ++pageId) {
            pageIds.push_back(pageId);
        }
        UNIT_ASSERT_VALUES_EQUAL(pageIds.size(), map.size());

        // Test emplace in some random order
        Shuffle(pageIds.begin(), pageIds.end());
        for (size_t i = 0; i < pageIds.size(); ++i) {
            const ui32 pageId = pageIds[i];
            //Cerr << "Emplacing page " << pageId << " at index " << i << Endl;
            UNIT_ASSERT(map.emplace(pageId, MakeHolder<TPage>(pageId)));
            UNIT_ASSERT(map[pageId]);
            UNIT_ASSERT_VALUES_EQUAL(map[pageId]->PageId, pageId);

            if (i > 0) {
                // Verify lookup of random pas element succeeds
                size_t oldIndex = RandomNumber<size_t>() % i;
                const ui32 oldPageId = pageIds[oldIndex];
                //Cerr << "Checking page " << oldPageId << " at index " << oldIndex << Endl;
                UNIT_ASSERT_C(map[oldPageId],
                    "Missing expected page " << oldPageId << " at index " << oldIndex <<
                    " after emplacing page " << pageId << " at index " << i);
                UNIT_ASSERT_VALUES_EQUAL(map[oldPageId]->PageId, oldPageId);
                UNIT_ASSERT_C(!map.emplace(oldPageId, MakeHolder<TPage>(-1)),
                    "Unexpected emplace of page " << oldPageId << " at index " << oldIndex <<
                    " after emplacing page " << pageId << " at index " << i);
            }

            if (i + 1 < pageIds.size()) {
                // Verify lookup of random future element fails
                size_t newIndex = i + 1 + RandomNumber<size_t>() % (pageIds.size() - i - 1);
                const ui32 newPageId = pageIds[newIndex];
                //Cerr << "Checking page " << newPageId << " at index " << newIndex << Endl;
                UNIT_ASSERT_C(!map[newPageId],
                    "Found unexpected page " << newPageId << " at index " << newIndex <<
                    " after emplacing page " << pageId << " at index " << i);
                UNIT_ASSERT_C(!map.erase(newPageId),
                    "Unexpected erase of page " << newPageId << " at index " << newIndex <<
                    " after emplacing page " << pageId << " at index " << i);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(map.used(), pageIds.size());

        // When full the map should stop being a hash table
        UNIT_ASSERT(!map.IsHashTable());

        // Test erase in some random order
        Shuffle(pageIds.begin(), pageIds.end());
        for (size_t i = 0; i < pageIds.size(); ++i) {
            UNIT_ASSERT(map[pageIds[i]]);
            UNIT_ASSERT_VALUES_EQUAL(map[pageIds[i]]->PageId, pageIds[i]);
            UNIT_ASSERT(map.erase(pageIds[i]));
            UNIT_ASSERT(!map[pageIds[i]]);
        }
        UNIT_ASSERT_VALUES_EQUAL(map.used(), 0u);
    }

    struct TSharedPage : public TThrRefBase {
        ui32 PageId;

        explicit TSharedPage(ui32 pageId)
            : PageId(pageId)
        { }
    };

    Y_UNIT_TEST(TestIntrusive) {
        TPageMap<TIntrusivePtr<TSharedPage>> map;
        map.resize(1024);
        TIntrusivePtr<TSharedPage> page1 = new TSharedPage(1);
        TIntrusivePtr<TSharedPage> page2 = new TSharedPage(2);
        UNIT_ASSERT(map.emplace(1, page1));
        UNIT_ASSERT(map.emplace(2, page2));
        UNIT_ASSERT_VALUES_EQUAL(page1->RefCount(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(page2->RefCount(), 2u);
        UNIT_ASSERT(map[1].Get() == page1.Get());
        UNIT_ASSERT(map[2].Get() == page2.Get());
        UNIT_ASSERT(map[3].Get() == nullptr);
        for (const auto& kv : map) {
            switch (kv.first) {
                case 1: UNIT_ASSERT(kv.second.Get() == page1.Get()); break;
                case 2: UNIT_ASSERT(kv.second.Get() == page2.Get()); break;
            }
        }
        UNIT_ASSERT(map.erase(1));
        UNIT_ASSERT(map.erase(2));
        UNIT_ASSERT_VALUES_EQUAL(page1->RefCount(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(page2->RefCount(), 1u);
        UNIT_ASSERT(map[1].Get() == nullptr);
        UNIT_ASSERT(map[2].Get() == nullptr);
        UNIT_ASSERT(map[3].Get() == nullptr);
        for (const auto& kv : map) {
            UNIT_ASSERT_C(false, "Unexpected page " << kv.first <<  " found in map");
        }
    }

    Y_UNIT_TEST(TestSimplePointer) {
        TPageMap<TPage*> map;
        map.resize(1024);
        TPage page1(1);
        TPage page2(2);
        UNIT_ASSERT(map.emplace(1, &page1));
        UNIT_ASSERT(map.emplace(2, &page2));
        UNIT_ASSERT(map[1] == &page1);
        UNIT_ASSERT(map[2] == &page2);
        UNIT_ASSERT(map[3] == nullptr);
        for (const auto& kv : map) {
            switch (kv.first) {
                case 1: UNIT_ASSERT(kv.second == &page1); break;
                case 2: UNIT_ASSERT(kv.second == &page2); break;
            }
        }
        UNIT_ASSERT(map.erase(1));
        UNIT_ASSERT(map.erase(2));
        UNIT_ASSERT(map[1] == nullptr);
        UNIT_ASSERT(map[2] == nullptr);
        UNIT_ASSERT(map[3] == nullptr);
        for (const auto& kv : map) {
            UNIT_ASSERT_C(false, "Unexpected page " << kv.first <<  " found in map");
        }
    }

    Y_UNIT_TEST(TestSharedPointer) {
        TPageMap<std::shared_ptr<TPage>> map;
        map.resize(1024);
        UNIT_ASSERT(map.emplace(1, std::make_shared<TPage>(1u)));
        UNIT_ASSERT(map.emplace(2, std::make_shared<TPage>(2u)));
        UNIT_ASSERT(map[1] && map[1]->PageId == 1);
        UNIT_ASSERT(map[2] && map[2]->PageId == 2);
        UNIT_ASSERT(!map[3]);
        for (const auto& kv : map) {
            UNIT_ASSERT_VALUES_EQUAL(kv.second->PageId, kv.first);
        }
        UNIT_ASSERT(map.erase(1));
        UNIT_ASSERT(map.erase(2));
        UNIT_ASSERT(!map[1]);
        UNIT_ASSERT(!map[2]);
        UNIT_ASSERT(!map[3]);
        for (const auto& kv : map) {
            UNIT_ASSERT_C(false, "Unexpected page " << kv.first <<  " found in map");
        }
    }

    Y_UNIT_TEST(TestSimplePointerFull) {
        TPageMap<TPage*> map;
        map.resize(1024 * 1024);

        TVector<TPage> pages(Reserve(map.size()));
        for (ui32 pageId = 0; pageId < map.size(); ++pageId) {
            pages.emplace_back(pageId);
        }

        for (auto& page : pages) {
            UNIT_ASSERT(map.emplace(page.PageId, &page));
        }
        UNIT_ASSERT_VALUES_EQUAL(map.used(), map.size());

        for (auto& page : pages) {
            UNIT_ASSERT(map[page.PageId] == &page);
            UNIT_ASSERT(map.erase(page.PageId));
        }
        UNIT_ASSERT_VALUES_EQUAL(map.used(), 0u);

        for (const auto& kv : map) {
            UNIT_ASSERT_C(false, "Unexpected page " << kv.first <<  " found in map");
        }
    }
}

}
