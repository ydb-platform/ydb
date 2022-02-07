#include "intrusive_heap.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

struct TMyItem {
    int Value;
    size_t HeapIndex = -1;

    explicit TMyItem(int value = 0)
        : Value(value)
    { }

    friend bool operator<(const TMyItem& a, const TMyItem& b) {
        return a.Value < b.Value;
    }

    struct THeapIndex {
        size_t& operator()(TMyItem& item) const {
            return item.HeapIndex;
        }
    };
};

Y_UNIT_TEST_SUITE(TIntrusiveHeapTest) {

    Y_UNIT_TEST(TestEmpty) {
        TIntrusiveHeap<TMyItem, TMyItem::THeapIndex> heap;

        UNIT_ASSERT(!heap);
        UNIT_ASSERT(heap.Empty());
        UNIT_ASSERT_VALUES_EQUAL(heap.Size(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(heap.Top(), nullptr);
    }

    Y_UNIT_TEST(TestAddRemove) {
        TVector<TMyItem> items;
        items.emplace_back(10); // 0
        items.emplace_back(20); // 1
        items.emplace_back(30); // 2
        items.emplace_back(40); // 3
        items.emplace_back(50); // 4
        items.emplace_back(60); // 5
        items.emplace_back(70); // 6

        TIntrusiveHeap<TMyItem, TMyItem::THeapIndex> heap;
        for (int i = 6; i >= 0; --i) {
            // Add all items in reverse order
            UNIT_ASSERT(!heap.Has(&items[i]));
            heap.Add(&items[i]);
            UNIT_ASSERT(heap.Has(&items[i]));
        }
        UNIT_ASSERT(heap);
        UNIT_ASSERT(!heap.Empty());
        UNIT_ASSERT_VALUES_EQUAL(heap.Size(), 7u);
        UNIT_ASSERT_VALUES_EQUAL(heap.Top(), &items[0]);

        // Expected heap order is:
        // 10, 40, 20, 70, 50, 60, 30
        UNIT_ASSERT_VALUES_EQUAL(items[0].HeapIndex, 0u);
        UNIT_ASSERT_VALUES_EQUAL(items[1].HeapIndex, 2u);
        UNIT_ASSERT_VALUES_EQUAL(items[2].HeapIndex, 6u);
        UNIT_ASSERT_VALUES_EQUAL(items[3].HeapIndex, 1u);
        UNIT_ASSERT_VALUES_EQUAL(items[4].HeapIndex, 4u);
        UNIT_ASSERT_VALUES_EQUAL(items[5].HeapIndex, 5u);
        UNIT_ASSERT_VALUES_EQUAL(items[6].HeapIndex, 3u);

        // Remove item with value 40
        UNIT_ASSERT(heap.Has(&items[3]));
        heap.Remove(&items[3]);
        UNIT_ASSERT(!heap.Has(&items[3]));
        UNIT_ASSERT_VALUES_EQUAL(heap.Size(), 6u);

        // Expected heap order transformations:
        // 10, xx, 20, 70, 50, 60, 30
        // 10, 50, 20, 70, xx, 60, 30
        // 10, 50, 20, 70, 30, 60, xx
        // 10, 30, 20, 70, 50, 60, xx
        UNIT_ASSERT_VALUES_EQUAL(heap.Top(), &items[0]);
        UNIT_ASSERT_VALUES_EQUAL(items[0].HeapIndex, 0u);
        UNIT_ASSERT_VALUES_EQUAL(items[1].HeapIndex, 2u);
        UNIT_ASSERT_VALUES_EQUAL(items[2].HeapIndex, 1u);
        UNIT_ASSERT_VALUES_EQUAL(items[4].HeapIndex, 4u);
        UNIT_ASSERT_VALUES_EQUAL(items[5].HeapIndex, 5u);
        UNIT_ASSERT_VALUES_EQUAL(items[6].HeapIndex, 3u);

        // Remove item with value 10
        UNIT_ASSERT(heap.Has(&items[0]));
        heap.Remove(&items[0]);
        UNIT_ASSERT(!heap.Has(&items[0]));
        UNIT_ASSERT_VALUES_EQUAL(heap.Size(), 5u);

        // Expected heap order transformations:
        // xx, 30, 20, 70, 50, 60
        // 20, 30, xx, 70, 50, 60
        // 20, 30, 60, 70, 50, xx
        UNIT_ASSERT_VALUES_EQUAL(heap.Top(), &items[1]);
        UNIT_ASSERT_VALUES_EQUAL(items[1].HeapIndex, 0u);
        UNIT_ASSERT_VALUES_EQUAL(items[2].HeapIndex, 1u);
        UNIT_ASSERT_VALUES_EQUAL(items[4].HeapIndex, 4u);
        UNIT_ASSERT_VALUES_EQUAL(items[5].HeapIndex, 2u);
        UNIT_ASSERT_VALUES_EQUAL(items[6].HeapIndex, 3u);
    }

    Y_UNIT_TEST(TestUpdateNoChange) {
        TVector<TMyItem> items;
        items.emplace_back(10); // 0
        items.emplace_back(20); // 1
        items.emplace_back(30); // 2
        items.emplace_back(40); // 3
        items.emplace_back(50); // 4
        items.emplace_back(60); // 5
        items.emplace_back(70); // 6

        TIntrusiveHeap<TMyItem, TMyItem::THeapIndex> heap;
        for (auto& item : items) {
            // Add all items in order
            heap.Add(&item);
        }

        // Check no change in all elements
        for (auto& item : items) {
            item.Value -= 10; // make it equal to the parent element
            UNIT_ASSERT(!heap.Update(&item));
            item.Value += 20; // make it equal to the child element
            UNIT_ASSERT(!heap.Update(&item));
            item.Value -= 10;
        }
    }

    Y_UNIT_TEST(TestUpdateIncrease) {
        TVector<TMyItem> items;
        items.emplace_back(10); // 0
        items.emplace_back(20); // 1
        items.emplace_back(30); // 2
        items.emplace_back(40); // 3
        items.emplace_back(50); // 4
        items.emplace_back(60); // 5
        items.emplace_back(70); // 6

        TIntrusiveHeap<TMyItem, TMyItem::THeapIndex> heap;
        for (auto& item : items) {
            // Add all items in order
            heap.Add(&item);
        }

        items[1].Value = 100; // 20 -> 100
        UNIT_ASSERT(heap.Update(&items[1]));

        // Expected heap transformations:
        // 10, 100, 30, 40, 50, 60, 70
        // 10, 40, 30, 100, 50, 60, 70
        UNIT_ASSERT_VALUES_EQUAL(items[1].HeapIndex, 3u);
        UNIT_ASSERT_VALUES_EQUAL(items[3].HeapIndex, 1u);

        items[0].Value = 200; // 10 -> 200
        UNIT_ASSERT(heap.Update(&items[0]));

        // Expected heap transformations:
        // 200, 40, 30, 100, 50, 60, 70
        // 30, 40, 200, 100, 50, 60, 70
        // 30, 40, 60, 100, 50, 200, 70
        UNIT_ASSERT_VALUES_EQUAL(items[0].HeapIndex, 5u);
        UNIT_ASSERT_VALUES_EQUAL(items[1].HeapIndex, 3u);
        UNIT_ASSERT_VALUES_EQUAL(items[2].HeapIndex, 0u);
        UNIT_ASSERT_VALUES_EQUAL(items[3].HeapIndex, 1u);
        UNIT_ASSERT_VALUES_EQUAL(items[4].HeapIndex, 4u);
        UNIT_ASSERT_VALUES_EQUAL(items[5].HeapIndex, 2u);
        UNIT_ASSERT_VALUES_EQUAL(items[6].HeapIndex, 6u);

        UNIT_ASSERT_VALUES_EQUAL(heap.Top(), &items[2]);
    }

    Y_UNIT_TEST(TestUpdateDecrease) {
        TVector<TMyItem> items;
        items.emplace_back(10); // 0
        items.emplace_back(20); // 1
        items.emplace_back(30); // 2
        items.emplace_back(40); // 3
        items.emplace_back(50); // 4
        items.emplace_back(60); // 5
        items.emplace_back(70); // 6

        TIntrusiveHeap<TMyItem, TMyItem::THeapIndex> heap;
        for (auto& item : items) {
            // Add all items in order
            heap.Add(&item);
        }

        items[6].Value = 0; // 70 -> 0
        UNIT_ASSERT(heap.Update(&items[6]));

        // Expected heap transformations:
        // 10, 20, 30, 40, 50, 60, 0
        // 10, 20, 0, 40, 50, 60, 30
        // 0, 20, 10, 40, 50, 60, 30
        UNIT_ASSERT_VALUES_EQUAL(items[0].HeapIndex, 2u);
        UNIT_ASSERT_VALUES_EQUAL(items[1].HeapIndex, 1u);
        UNIT_ASSERT_VALUES_EQUAL(items[2].HeapIndex, 6u);
        UNIT_ASSERT_VALUES_EQUAL(items[3].HeapIndex, 3u);
        UNIT_ASSERT_VALUES_EQUAL(items[4].HeapIndex, 4u);
        UNIT_ASSERT_VALUES_EQUAL(items[5].HeapIndex, 5u);
        UNIT_ASSERT_VALUES_EQUAL(items[6].HeapIndex, 0u);

        UNIT_ASSERT_VALUES_EQUAL(heap.Top(), &items[6]);
    }

}

}   // namespace NKikimr
