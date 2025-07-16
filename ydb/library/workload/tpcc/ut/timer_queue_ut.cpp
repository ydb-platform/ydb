#include <ydb/library/workload/tpcc/timer_queue.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/gmock_in_unittest/gmock.h>

#include <vector>
#include <algorithm>
#include <random>
#include <chrono>

using namespace NYdb;
using namespace NYdb::NTPCC;

Y_UNIT_TEST_SUITE(TBinnedTimerQueueTest) {
    Y_UNIT_TEST(ShouldAddSingleItem) {
        TBinnedTimerQueue<int> queue(10, 100);
        queue.Add(std::chrono::milliseconds(10), 42);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 1UL);
        UNIT_ASSERT(queue.Validate());
    }

    Y_UNIT_TEST(ShouldPopItemInOrder) {
        TBinnedTimerQueue<int> queue(10, 100);
        queue.Add(std::chrono::milliseconds(1), 1);
        queue.Add(std::chrono::milliseconds(2), 2);
        queue.Add(std::chrono::milliseconds(3), 3);
        UNIT_ASSERT(queue.Validate());

        auto a = queue.PopFront().Value;
        UNIT_ASSERT(queue.Validate());
        auto b = queue.PopFront().Value;
        UNIT_ASSERT(queue.Validate());
        auto c = queue.PopFront().Value;

        UNIT_ASSERT_VALUES_EQUAL(a, 1);
        UNIT_ASSERT_VALUES_EQUAL(b, 2);
        UNIT_ASSERT_VALUES_EQUAL(c, 3);
        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0UL);
        UNIT_ASSERT(queue.Validate());
    }

    Y_UNIT_TEST(ShouldInsertOutOfOrderAndStillPopInOrder) {
        TBinnedTimerQueue<int> queue(10, 100);
        queue.Add(std::chrono::milliseconds(20), 2);
        UNIT_ASSERT(queue.Validate());
        queue.Add(std::chrono::milliseconds(10), 1);
        UNIT_ASSERT(queue.Validate());
        queue.Add(std::chrono::milliseconds(30), 3);
        UNIT_ASSERT(queue.Validate());

        UNIT_ASSERT_VALUES_EQUAL(queue.PopFront().Value, 1);
        UNIT_ASSERT(queue.Validate());

        UNIT_ASSERT_VALUES_EQUAL(queue.PopFront().Value, 2);
        UNIT_ASSERT(queue.Validate());

        UNIT_ASSERT_VALUES_EQUAL(queue.PopFront().Value, 3);
        UNIT_ASSERT(queue.Validate());
    }

    Y_UNIT_TEST(ShouldAdvanceWhenBucketExhausted) {
        TBinnedTimerQueue<int> queue(10, 100);
        queue.Add(std::chrono::milliseconds(1), 11);
        UNIT_ASSERT(queue.Validate());

        queue.Add(std::chrono::milliseconds(200), 22); // will go to later bucket
        UNIT_ASSERT(queue.Validate());

        UNIT_ASSERT_VALUES_EQUAL(queue.PopFront().Value, 11);
        UNIT_ASSERT(queue.Validate());

        UNIT_ASSERT_VALUES_EQUAL(queue.PopFront().Value, 22); // triggers advance
        UNIT_ASSERT(queue.Validate());

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 0UL);
        UNIT_ASSERT(queue.Validate());
    }

    Y_UNIT_TEST(ShouldThrowOnEmptyPop) {
        TBinnedTimerQueue<int> queue(4, 100);
        UNIT_ASSERT(queue.Validate());

        UNIT_ASSERT_EXCEPTION(queue.PopFront(), std::runtime_error);
    }

    Y_UNIT_TEST(ShouldRespectBucketSoftLimit) {
        TBinnedTimerQueue<int> queue(4, 1);
        // These two will go into separate buckets due to limit
        queue.Add(std::chrono::milliseconds(100), 1);
        UNIT_ASSERT(queue.Validate());

        queue.Add(std::chrono::milliseconds(100), 2);
        UNIT_ASSERT(queue.Validate());

        UNIT_ASSERT_VALUES_EQUAL(queue.Size(), 2UL);
        UNIT_ASSERT_VALUES_EQUAL(queue.PopFront().Value, 1);
        UNIT_ASSERT(queue.Validate());

        UNIT_ASSERT_VALUES_EQUAL(queue.PopFront().Value, 2);
        UNIT_ASSERT(queue.Validate());
    }

    Y_UNIT_TEST(ShouldPopInStrictOrderAfterShuffledInsertion) {
        TBinnedTimerQueue<int> queue(32, 29);

        std::vector<int> input;
        for (int i = 1; i <= 9999; ++i) {
            input.push_back(i);
        }

        std::mt19937 rng{12345};
        std::shuffle(input.begin(), input.end(), rng);

        for (int v: input) {
            queue.Add(std::chrono::milliseconds(v), v, std::chrono::steady_clock::time_point{});
            UNIT_ASSERT(queue.Validate());
        }

        for (int i = 1; i <= 999; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(queue.PopFront().Value, i);
            UNIT_ASSERT(queue.Validate());
        }
    }

    Y_UNIT_TEST(ShouldDistributeTimersAcrossBuckets) {
        TBinnedTimerQueue<int> queue(8, 4);
        for (int i = 0; i < 8; ++i) {
            queue.Add(std::chrono::milliseconds(i * 100), i);
            UNIT_ASSERT(queue.Validate());
        }

        for (int i = 0; i < 8; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(queue.PopFront().Value, i);
            UNIT_ASSERT(queue.Validate());
        }
    }

    Y_UNIT_TEST(ShouldHandleBucketBoundaries) {
        TBinnedTimerQueue<int> queue(4, 100);
        auto now = std::chrono::steady_clock::now();

        // Fill first bucket
        for (int i = 0; i < 100; ++i) {
            queue.Add(std::chrono::milliseconds(i), i, now);
        }

        // Add items at bucket boundaries
        queue.Add(std::chrono::milliseconds(100), 100, now);
        queue.Add(std::chrono::milliseconds(101), 101, now);

        // Verify order
        for (int i = 0; i <= 101; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(queue.PopFront().Value, i);
            UNIT_ASSERT(queue.Validate());
        }
    }

    Y_UNIT_TEST(ShouldHandleBucketOverflow) {
        TBinnedTimerQueue<int> queue(4, 2); // Small bucket limit

        // Add more items than bucket limit
        for (int i = 0; i < 10; ++i) {
            queue.Add(std::chrono::milliseconds(i * 100), i);
            UNIT_ASSERT(queue.Validate());
        }

        // Verify all items are popped in order
        for (int i = 0; i < 10; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(queue.PopFront().Value, i);
            UNIT_ASSERT(queue.Validate());
        }
    }

    Y_UNIT_TEST(ShouldHandleBucketRotation) {
        TBinnedTimerQueue<int> queue(3, 100);

        // Fill all buckets with items at specific time points
        for (int i = 0; i < 300; ++i) {
            // Use fixed time points to ensure proper bucket distribution
            auto timePoint = std::chrono::steady_clock::time_point{} + std::chrono::milliseconds(i * 100);
            queue.Add(std::chrono::milliseconds(i * 100), i, timePoint);

            // Debug output on validation failure
            if (!queue.Validate()) {
                std::cerr << "Validation failed after adding item " << i << std::endl;
                queue.Dump();
                UNIT_ASSERT(queue.Validate());
            }
        }

        // Pop all items and verify bucket rotation
        for (int i = 0; i < 300; ++i) {
            auto item = queue.PopFront();
            UNIT_ASSERT_VALUES_EQUAL(item.Value, i);

            // Debug output on validation failure
            if (!queue.Validate()) {
                std::cerr << "Validation failed after popping item " << i << std::endl;
                queue.Dump();
                UNIT_ASSERT(queue.Validate());
            }
        }
    }

    Y_UNIT_TEST(ShouldHandleBucketRotationWithFixedTimePoints) {
        TBinnedTimerQueue<int> queue(3, 100);
        auto baseTime = std::chrono::steady_clock::time_point{};

        // Add items with fixed time points to ensure proper bucket distribution
        for (int i = 0; i < 9; ++i) {
            auto timePoint = baseTime + std::chrono::milliseconds(i * 1000);
            queue.Add(std::chrono::milliseconds(i * 1000), i, timePoint);
            UNIT_ASSERT(queue.Validate());
        }

        // Verify items are popped in order
        for (int i = 0; i < 9; ++i) {
            auto item = queue.PopFront();
            UNIT_ASSERT_VALUES_EQUAL(item.Value, i);
            UNIT_ASSERT(queue.Validate());
        }
    }
}
