#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/vector.h>
#include <util/system/thread.h>

#include "ut_helpers.h"

template <typename TQueueType>
class TQueueTestsInSingleThread: public TTestBase {
private:
    using TSelf = TQueueTestsInSingleThread<TQueueType>;
    using TLink = TIntrusiveLink;

    UNIT_TEST_SUITE_DEMANGLE(TSelf);
    UNIT_TEST(OnePushOnePop)
    UNIT_TEST(OnePushOnePop_Repeat1M)
    UNIT_TEST(Threads8_Repeat1M_Push1Pop1)
    UNIT_TEST_SUITE_END();

public:
    void OnePushOnePop() {
        TQueueType queue;

        auto popped = queue.Pop();
        UNIT_ASSERT_VALUES_EQUAL(popped, nullptr);

        TLink msg;
        queue.Push(&msg);
        popped = queue.Pop();
        UNIT_ASSERT_VALUES_EQUAL(&msg, popped);

        popped = queue.Pop();
        UNIT_ASSERT_VALUES_EQUAL(popped, nullptr);
    }

    void OnePushOnePop_Repeat1M() {
        TQueueType queue;
        TLink msg;

        auto popped = queue.Pop();
        UNIT_ASSERT_VALUES_EQUAL(popped, nullptr);

        for (int i = 0; i < 1000000; ++i) {
            queue.Push(&msg);
            popped = queue.Pop();
            UNIT_ASSERT_VALUES_EQUAL(&msg, popped);

            popped = queue.Pop();
            UNIT_ASSERT_VALUES_EQUAL(popped, nullptr);
        }
    }

    template <size_t NUMBER_OF_THREADS>
    void RepeatPush1Pop1_InManyThreads() {
        class TCycleThread: public ISimpleThread {
        public:
            void* ThreadProc() override {
                TQueueType queue;
                TLink msg;
                auto popped = queue.Pop();
                UNIT_ASSERT_VALUES_EQUAL(popped, nullptr);

                for (size_t i = 0; i < 1000000; ++i) {
                    queue.Push(&msg);
                    popped = queue.Pop();
                    UNIT_ASSERT_VALUES_EQUAL(popped, &msg);

                    popped = queue.Pop();
                    UNIT_ASSERT_VALUES_EQUAL(popped, nullptr);
                }
                return nullptr;
            }
        };

        TVector<TAutoPtr<TCycleThread>> cyclers;

        for (size_t i = 0; i < NUMBER_OF_THREADS; ++i) {
            cyclers.emplace_back(new TCycleThread);
            cyclers.back()->Start();
        }

        for (size_t i = 0; i < NUMBER_OF_THREADS; ++i) {
            cyclers[i]->Join();
        }
    }

    void Threads8_Repeat1M_Push1Pop1() {
        RepeatPush1Pop1_InManyThreads<8>();
    }
};

REGISTER_TESTS_FOR_ALL_ORDERED_QUEUES(TQueueTestsInSingleThread);
REGISTER_TESTS_FOR_ALL_UNORDERED_QUEUES(TQueueTestsInSingleThread)
