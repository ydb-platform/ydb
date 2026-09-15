#include "spsc_circular_queue.h"

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <thread>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(TSpscCircularQueueTestSingleThread) {
    Y_UNIT_TEST(ShouldReportEmptyInitially) {
        TSpscCircularQueue<int> queue;
        queue.Resize(3);
        UNIT_ASSERT(queue.Empty());
        UNIT_ASSERT(!queue.IsFull());
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0);
    }

    Y_UNIT_TEST(ShouldPushAndPopSingleItem) {
        TSpscCircularQueue<int> queue;
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
        TSpscCircularQueue<int> queue;
        queue.Resize(2);
        UNIT_ASSERT(queue.TryPush(1));
        UNIT_ASSERT(queue.TryPush(2));
        UNIT_ASSERT(!queue.TryPush(3)); // Full

        UNIT_ASSERT(queue.IsFull());
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2);
    }

    Y_UNIT_TEST(ShouldRejectPopWhenEmpty) {
        TSpscCircularQueue<int> queue;
        queue.Resize(2);
        int value = 0;
        UNIT_ASSERT(!queue.TryPop(value)); // Nothing to pop
    }

    Y_UNIT_TEST(ShouldPreserveFIFOOrder) {
        TSpscCircularQueue<int> queue;
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
        TSpscCircularQueue<int> queue;
        queue.Resize(4);

        UNIT_ASSERT(queue.TryPush(1));
        UNIT_ASSERT(queue.TryPush(2));
        int val;
        UNIT_ASSERT(queue.TryPop(val)); // remove 1
        UNIT_ASSERT(queue.TryPop(val)); // remove 2

        // now push again, it should wrap around
        UNIT_ASSERT(queue.TryPush(3));
        UNIT_ASSERT(queue.TryPush(4));
        UNIT_ASSERT(queue.TryPush(5));
        UNIT_ASSERT(queue.TryPush(6)); // fills it

        UNIT_ASSERT(!queue.TryPush(7)); // full again

        UNIT_ASSERT(queue.TryPop(val));
        UNIT_ASSERT_VALUES_EQUAL(val, 3);
        UNIT_ASSERT(queue.TryPop(val));
        UNIT_ASSERT_VALUES_EQUAL(val, 4);
        UNIT_ASSERT(queue.TryPop(val));
        UNIT_ASSERT_VALUES_EQUAL(val, 5);
        UNIT_ASSERT(queue.TryPop(val));
        UNIT_ASSERT_VALUES_EQUAL(val, 6);
    }

    Y_UNIT_TEST(ShouldResizeThenOperate) {
        TSpscCircularQueue<TString> queue;
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

    Y_UNIT_TEST(ShouldRoundCapacityUpToPowerOfTwo) {
        TSpscCircularQueue<int> queue;
        queue.Resize(3);

        UNIT_ASSERT(queue.TryPush(1));
        UNIT_ASSERT(queue.TryPush(2));
        UNIT_ASSERT(queue.TryPush(3));
        UNIT_ASSERT(queue.TryPush(4));
        UNIT_ASSERT(!queue.TryPush(5));
    }
}

Y_UNIT_TEST_SUITE(TSpscCircularQueueTestTwoThreads) {
    Y_UNIT_TEST(ShouldTransferAllItemsInOrder) {
        constexpr size_t capacity = 1024;
        constexpr size_t itemCount = 200000;

        TSpscCircularQueue<size_t> queue;
        queue.Resize(capacity);

        std::atomic<bool> orderBroken = false;
        std::atomic<size_t> firstExpected = 0;
        std::atomic<size_t> firstActual = 0;
        std::atomic<size_t> consumed = 0;
        std::atomic<unsigned long long> consumedSum = 0;

        std::thread producer([&] {
            for (size_t i = 0; i < itemCount; ++i) {
                while (!queue.TryPush(size_t{i})) {
                    std::this_thread::yield();
                }
            }
        });

        std::thread consumer([&] {
            size_t expected = 0;
            while (expected < itemCount) {
                size_t value = 0;
                if (!queue.TryPop(value)) {
                    std::this_thread::yield();
                    continue;
                }

                if (value != expected && !orderBroken.exchange(true)) {
                    firstExpected.store(expected);
                    firstActual.store(value);
                }

                ++expected;
                consumed.fetch_add(1);
                consumedSum.fetch_add(static_cast<unsigned long long>(value));
            }
        });

        producer.join();
        consumer.join();

        UNIT_ASSERT_C(!orderBroken.load(),
            "FIFO order broken: expected "
            << firstExpected.load()
            << ", got "
            << firstActual.load());
        UNIT_ASSERT_VALUES_EQUAL(consumed.load(), itemCount);

        const auto expectedSum = static_cast<unsigned long long>(itemCount - 1) * itemCount / 2;
        UNIT_ASSERT_VALUES_EQUAL(consumedSum.load(), expectedSum);
        UNIT_ASSERT(queue.Empty());
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0);
    }

    Y_UNIT_TEST(ShouldWorkWithCapacityOneUnderContention) {
        constexpr size_t capacity = 1;
        constexpr size_t itemCount = 50000;

        TSpscCircularQueue<size_t> queue;
        queue.Resize(capacity);

        std::atomic<bool> orderBroken = false;
        std::atomic<size_t> firstExpected = 0;
        std::atomic<size_t> firstActual = 0;
        std::atomic<size_t> consumed = 0;

        std::thread producer([&] {
            for (size_t i = 0; i < itemCount; ++i) {
                while (!queue.TryPush(size_t{i})) {
                    std::this_thread::yield();
                }
            }
        });

        std::thread consumer([&] {
            size_t expected = 0;
            while (expected < itemCount) {
                size_t value = 0;
                if (!queue.TryPop(value)) {
                    std::this_thread::yield();
                    continue;
                }

                if (value != expected && !orderBroken.exchange(true)) {
                    firstExpected.store(expected);
                    firstActual.store(value);
                }

                ++expected;
                consumed.fetch_add(1);
            }
        });

        producer.join();
        consumer.join();

        UNIT_ASSERT_C(!orderBroken.load(),
            "FIFO order broken with capacity=1: expected "
            << firstExpected.load()
            << ", got "
            << firstActual.load());
        UNIT_ASSERT_VALUES_EQUAL(consumed.load(), itemCount);
        UNIT_ASSERT(queue.Empty());
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0);
    }
}