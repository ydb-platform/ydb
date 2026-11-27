#include "bounded_queue.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/thread/factory.h>

using namespace NThreading;

Y_UNIT_TEST_SUITE(TBoundedQueueTest) {
    Y_UNIT_TEST(QueueSize) {
        const size_t queueSize = 16;
        TBoundedQueue<size_t> boundedQueue(queueSize);

        for (size_t i = 0; i < queueSize; ++i) {
            UNIT_ASSERT(boundedQueue.Enqueue(i));
        }
        UNIT_ASSERT(!boundedQueue.Enqueue(0));
        size_t tmp = 0;
        UNIT_ASSERT(boundedQueue.Dequeue(tmp));
        UNIT_ASSERT(boundedQueue.Enqueue(0));
        UNIT_ASSERT(!boundedQueue.Enqueue(0));
    }

    Y_UNIT_TEST(Move) {
        const size_t queueSize = 16;
        TBoundedQueue<TString> boundedQueue(queueSize);

        for (size_t i = 0; i < queueSize; ++i) {
            TString v = "xxx";
            UNIT_ASSERT(boundedQueue.Enqueue(std::move(v)));
            UNIT_ASSERT(v.empty());
        }

        {
            TString v = "xxx";
            UNIT_ASSERT(!boundedQueue.Enqueue(std::move(v)));
            UNIT_ASSERT(v == "xxx");
        }

        TString v;
        UNIT_ASSERT(boundedQueue.Dequeue(v));
        UNIT_ASSERT(v == "xxx");
    }

    Y_UNIT_TEST(MPMC) {
        size_t queueSize = 16;
        size_t producers = 10;
        size_t consumers = 10;
        size_t itemsCount = 10000;

        TVector<THolder<IThreadFactory::IThread>> threads;
        TBoundedQueue<std::pair<size_t, size_t>> boundedQueue(queueSize);

        std::atomic<size_t> itemCounter = 0;
        std::atomic<size_t> consumed = 0;

        for (size_t i = 0; i < consumers; ++i) {
            threads.push_back(SystemThreadFactory()->Run(
                [&]() {
                    TVector<size_t> prevItems(producers);
                    for (;;) {
                        std::pair<size_t, size_t> item;
                        while (!boundedQueue.Dequeue(item)) {
                            ;
                        }

                        if (item.first >= producers) {
                            break;
                        }

                        UNIT_ASSERT(item.second > prevItems[item.first]);
                        prevItems[item.first] = item.second;
                        ++consumed;
                    }
                })
            );
        }

        for (size_t i = 0; i < producers ; ++i) {
            threads.push_back(SystemThreadFactory()->Run(
                [&, producerNum = i]() {
                    for (;;) {
                        size_t item = ++itemCounter;
                        if (item > itemsCount) {
                            break;
                        }

                        while (!boundedQueue.Enqueue(std::make_pair(producerNum, item))) {
                            ;
                        }
                    }

                    while (!boundedQueue.Enqueue(std::make_pair(producers, size_t(0)))) {
                        ;
                    }
                })
            );
        }


        for (auto& t : threads) {
            t->Join();
        }

        UNIT_ASSERT_VALUES_EQUAL(consumed.load(), itemsCount);
    }
}
