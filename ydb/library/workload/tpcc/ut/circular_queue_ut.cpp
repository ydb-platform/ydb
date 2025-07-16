#include <ydb/library/workload/tpcc/circular_queue.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;
using namespace NYdb::NTPCC;

Y_UNIT_TEST_SUITE(TCircularQueueTest) {

    Y_UNIT_TEST(ShouldReportEmptyInitially) {
        TCircularQueue<int> queue;
        queue.Resize(3);
        UNIT_ASSERT(queue.Empty());
        UNIT_ASSERT(!queue.IsFull());
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0);
    }

    Y_UNIT_TEST(ShouldPushAndPopSingleItem) {
        TCircularQueue<int> queue;
        queue.Resize(1);

        UNIT_ASSERT(queue.TryPush(42));
        UNIT_ASSERT(!queue.Empty());
        UNIT_ASSERT(queue.IsFull());

        int value = 0;
        UNIT_ASSERT(queue.TryPop(value));
        UNIT_ASSERT_VALUES_EQUAL(value, 42);
        UNIT_ASSERT(queue.Empty());
    }

    Y_UNIT_TEST(ShouldRejectPushWhenFull) {
        TCircularQueue<int> queue;
        queue.Resize(2);
        UNIT_ASSERT(queue.TryPush(1));
        UNIT_ASSERT(queue.TryPush(2));
        UNIT_ASSERT(!queue.TryPush(3)); // Full

        UNIT_ASSERT(queue.IsFull());
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2);
    }

    Y_UNIT_TEST(ShouldRejectPopWhenEmpty) {
        TCircularQueue<int> queue;
        queue.Resize(2);
        int value = 0;
        UNIT_ASSERT(!queue.TryPop(value)); // Nothing to pop
    }

    Y_UNIT_TEST(ShouldPreserveFIFOOrder) {
        TCircularQueue<int> queue;
        queue.Resize(3);

        UNIT_ASSERT(queue.TryPush(10));
        UNIT_ASSERT(queue.TryPush(20));
        UNIT_ASSERT(queue.TryPush(30));

        int val;
        UNIT_ASSERT(queue.TryPop(val));
        UNIT_ASSERT_VALUES_EQUAL(val, 10);

        UNIT_ASSERT(queue.TryPop(val));
        UNIT_ASSERT_VALUES_EQUAL(val, 20);

        UNIT_ASSERT(queue.TryPop(val));
        UNIT_ASSERT_VALUES_EQUAL(val, 30);
    }

    Y_UNIT_TEST(ShouldWrapAroundProperly) {
        TCircularQueue<int> queue;
        queue.Resize(3);

        UNIT_ASSERT(queue.TryPush(1));
        UNIT_ASSERT(queue.TryPush(2));
        int val;
        UNIT_ASSERT(queue.TryPop(val)); // remove 1
        UNIT_ASSERT(queue.TryPop(val)); // remove 2

        // now push again, it should wrap around
        UNIT_ASSERT(queue.TryPush(3));
        UNIT_ASSERT(queue.TryPush(4));
        UNIT_ASSERT(queue.TryPush(5)); // fills it

        UNIT_ASSERT(!queue.TryPush(6)); // full again

        UNIT_ASSERT(queue.TryPop(val));
        UNIT_ASSERT_VALUES_EQUAL(val, 3);
        UNIT_ASSERT(queue.TryPop(val));
        UNIT_ASSERT_VALUES_EQUAL(val, 4);
        UNIT_ASSERT(queue.TryPop(val));
        UNIT_ASSERT_VALUES_EQUAL(val, 5);
    }

    Y_UNIT_TEST(ShouldResizeThenOperate) {
        TCircularQueue<TString> queue;
        queue.Resize(2);
        UNIT_ASSERT(queue.TryPush("a"));
        UNIT_ASSERT(queue.TryPush("b"));
        UNIT_ASSERT(!queue.TryPush("c")); // full

        TString val;
        UNIT_ASSERT(queue.TryPop(val));
        UNIT_ASSERT_VALUES_EQUAL(val, "a");
        UNIT_ASSERT(queue.TryPop(val));
        UNIT_ASSERT_VALUES_EQUAL(val, "b");
        UNIT_ASSERT(queue.Empty());
    }
}
