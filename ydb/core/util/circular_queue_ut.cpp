#include "circular_queue.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using TIntFifoQueue = TFifoQueue<int>;

Y_UNIT_TEST_SUITE(TFifoQueueTest) {
   Y_UNIT_TEST(ShouldPushPop) {
        TIntFifoQueue queue;
        UNIT_ASSERT(queue.PushBack(1));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 1);

        UNIT_ASSERT(queue.PushBack(10));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 1);

        queue.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 10);

        queue.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0UL);
    }
};

using TIntCircularQueue = TCircularQueue<int, TFifoQueue<int>>;

Y_UNIT_TEST_SUITE(TCircularQueueTest) {

    Y_UNIT_TEST(Empty) {
        TIntCircularQueue queue;
        UNIT_ASSERT(queue.Empty());
    }

    Y_UNIT_TEST(ShouldPush) {
        TIntCircularQueue queue;
        UNIT_ASSERT(queue.PushBack(1));
        UNIT_ASSERT(!queue.Empty());
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 1);

        UNIT_ASSERT(queue.PushBack(2));
        UNIT_ASSERT(!queue.Empty());
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 1);
    }

    Y_UNIT_TEST(ShouldNotPushTwice) {
        TIntCircularQueue queue;
        UNIT_ASSERT(queue.PushBack(1));
        UNIT_ASSERT(!queue.PushBack(1));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1UL);

        UNIT_ASSERT(queue.PushBack(2));
        UNIT_ASSERT(!queue.PushBack(2));
        UNIT_ASSERT(!queue.PushBack(1));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);
    }

    Y_UNIT_TEST(ShouldNextSingleItem) {
        TIntCircularQueue queue;
        queue.PushBack(1);

        queue.PopFront();
        UNIT_ASSERT(!queue.Empty());
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 1);

        queue.PopFront();
        UNIT_ASSERT(!queue.Empty());
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 1);
    }

    Y_UNIT_TEST(ShouldNextMulti) {
        TIntCircularQueue queue;
        queue.PushBack(1);
        queue.PushBack(2);

        queue.PopFront();
        UNIT_ASSERT(!queue.Empty());
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 2);

        queue.PopFront();
        UNIT_ASSERT(!queue.Empty());
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 1);

        queue.PopFront();
        UNIT_ASSERT(!queue.Empty());
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 2);
    }

    Y_UNIT_TEST(ShouldRemove) {
        TIntCircularQueue queue;
        queue.PushBack(1);
        queue.PushBack(2);
        queue.PushBack(3);

        UNIT_ASSERT(queue.Remove(2));
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 1);
        queue.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 3);
    }

    Y_UNIT_TEST(ShouldNotRemoveMissing) {
        TIntCircularQueue queue;
        queue.PushBack(1);
        queue.PushBack(2);
        queue.PushBack(3);
        UNIT_ASSERT(!queue.Remove(4));
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 3UL);
    }

    Y_UNIT_TEST(ShouldRemoveCurrent) {
        TIntCircularQueue queue;
        queue.PushBack(1);
        queue.PushBack(2);
        queue.PushBack(3);
        queue.PopFront();

        queue.Remove(2);
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 3);

        queue.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 1);

        queue.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 3);
    }

    Y_UNIT_TEST(ShouldRemoveCurrentLast) {
        TIntCircularQueue queue;
        queue.PushBack(1);
        queue.PushBack(2);
        queue.PushBack(3);

        queue.PopFront();
        queue.PopFront();

        queue.Remove(3);
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 1);

        queue.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 2);

        queue.PopFront();
        UNIT_ASSERT_VALUES_EQUAL(queue.Front(), 1);
    }

    Y_UNIT_TEST(ShouldGetQueue) {
        TIntCircularQueue queue;
        queue.PushBack(3);
        queue.PushBack(2);
        queue.PushBack(1);

        auto items = queue.GetQueue();
        UNIT_ASSERT_VALUES_EQUAL(items.size(), queue.Size());
        UNIT_ASSERT_VALUES_EQUAL(items, TVector<int>({3, 2, 1}));
    }
};
 
} // NKikimr
