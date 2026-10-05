#include "yql_qyt_blocking_queue.h"

#include <util/datetime/base.h>

#include <library/cpp/testing/unittest/registar.h>

#include <thread>

namespace NYql {

Y_UNIT_TEST_SUITE(TBlockingEQueueTest) {

    Y_UNIT_TEST(PushPop) {
        TBlockingEQueue<TString> queue(1024);
        queue.Push(TString("hello"), 5);
        queue.Push(TString("world"), 5);

        auto val1 = queue.Pop(/* block */ false);
        UNIT_ASSERT(val1.has_value());
        UNIT_ASSERT_EQUAL(*val1, TString("hello"));

        auto val2 = queue.Pop(/* block */ false);
        UNIT_ASSERT(val2.has_value());
        UNIT_ASSERT_EQUAL(*val2, TString("world"));

        auto val3 = queue.Pop(/* block */ false);
        UNIT_ASSERT(!val3.has_value());
    }

    Y_UNIT_TEST(Backpressure) {
        TBlockingEQueue<TString> queue(10); // max 10 bytes

        queue.Push(TString("hello"), 5);
        queue.Push(TString("world"), 5);
        // Queue is now at 10 bytes, next push should block

        bool pushed = false;
        auto pushThread = std::thread([&]() {
            queue.Push(TString("blocked"), 7);
            pushed = true;
        });

        // Give the push thread time to block
        Sleep(TDuration::MilliSeconds(100));
        UNIT_ASSERT_EQUAL(pushed, false);

        // Pop one item to free space
        queue.Pop(/* block */ false);

        // Now the push should complete
        pushThread.join();
        UNIT_ASSERT_EQUAL(pushed, true);
    }

    Y_UNIT_TEST(BlockUntilEvent) {
        TBlockingEQueue<TString> queue(1024);

        bool unblocked = false;
        auto waitThread = std::thread([&]() {
            queue.BlockUntilEvent();
            unblocked = true;
        });

        Sleep(TDuration::MilliSeconds(100));
        UNIT_ASSERT_EQUAL(unblocked, false);

        queue.Push(TString("wake up"), 8);
        waitThread.join();
        UNIT_ASSERT_EQUAL(unblocked, true);
    }

    Y_UNIT_TEST(Stop) {
        TBlockingEQueue<TString> queue(1024);
        queue.Stop();

        auto val = queue.Pop(/* block */ false);
        UNIT_ASSERT(!val.has_value());
    }

}

} // namespace NYql
