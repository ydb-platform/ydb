#include <library/cpp/testing/unittest/registar.h>
#include <util/system/thread.h>

#include "ut_helpers.h"

typedef void* TMsgLink;

template <typename TQueueType>
class TQueueTestProcs: public TTestBase {
private:
    UNIT_TEST_SUITE_DEMANGLE(TQueueTestProcs<TQueueType>);
    UNIT_TEST(Threads2_Push1M_Threads1_Pop2M)
    UNIT_TEST(Threads4_Push1M_Threads1_Pop4M)
    UNIT_TEST(Threads8_RndPush100K_Threads8_Queues)
    /*
    UNIT_TEST(Threads24_RndPush100K_Threads24_Queues)
    UNIT_TEST(Threads24_RndPush100K_Threads8_Queues)
    UNIT_TEST(Threads24_RndPush100K_Threads4_Queues)
*/
    UNIT_TEST_SUITE_END();

public:
    void Push1M_Pop1M() {
        TQueueType queue;
        TMsgLink msg = &queue;

        auto pmsg = queue.Pop();
        UNIT_ASSERT_VALUES_EQUAL(pmsg, nullptr);

        for (int i = 0; i < 1000000; ++i) {
            queue.Push((char*)msg + i);
        }

        for (int i = 0; i < 1000000; ++i) {
            auto popped = queue.Pop();
            UNIT_ASSERT_EQUAL((char*)msg + i, popped);
        }

        pmsg = queue.Pop();
        UNIT_ASSERT_VALUES_EQUAL(pmsg, nullptr);
    }

    void Threads2_Push1M_Threads1_Pop2M() {
        TQueueType queue;

        class TPusherThread: public ISimpleThread {
        public:
            TPusherThread(TQueueType& theQueue, char* start)
                : Queue(theQueue)
                , Arg(start)
            {
            }

            TQueueType& Queue;
            char* Arg;

            void* ThreadProc() override {
                for (int i = 0; i < 1000000; ++i) {
                    Queue.Push(Arg + i);
                }
                return nullptr;
            }
        };

        TPusherThread pusher1(queue, (char*)&queue);
        TPusherThread pusher2(queue, (char*)&queue + 2000000);

        pusher1.Start();
        pusher2.Start();

        for (int i = 0; i < 2000000; ++i) {
            while (queue.Pop() == nullptr) {
                SpinLockPause();
            }
        }

        auto pmsg = queue.Pop();
        UNIT_ASSERT_VALUES_EQUAL(pmsg, nullptr);
    }

    void Threads4_Push1M_Threads1_Pop4M() {
        TQueueType queue;

        class TPusherThread: public ISimpleThread {
        public:
            TPusherThread(TQueueType& theQueue, char* start)
                : Queue(theQueue)
                , Arg(start)
            {
            }

            TQueueType& Queue;
            char* Arg;

            void* ThreadProc() override {
                for (int i = 0; i < 1000000; ++i) {
                    Queue.Push(Arg + i);
                }
                return nullptr;
            }
        };

        TPusherThread pusher1(queue, (char*)&queue);
        TPusherThread pusher2(queue, (char*)&queue + 2000000);
        TPusherThread pusher3(queue, (char*)&queue + 4000000);
        TPusherThread pusher4(queue, (char*)&queue + 6000000);

        pusher1.Start();
        pusher2.Start();
        pusher3.Start();
        pusher4.Start();

        for (int i = 0; i < 4000000; ++i) {
            while (queue.Pop() == nullptr) {
                SpinLockPause();
            }
        }

        auto pmsg = queue.Pop();
        UNIT_ASSERT_VALUES_EQUAL(pmsg, nullptr);
    }

    template <size_t NUMBER_OF_PUSHERS, size_t NUMBER_OF_QUEUES>
    void ManyRndPush100K_ManyQueues() {
        TQueueType queue[NUMBER_OF_QUEUES];

        class TPusherThread: public ISimpleThread {
        public:
            TPusherThread(TQueueType* queues, char* start)
                : Queues(queues)
                , Arg(start)
            {
            }

            TQueueType* Queues;
            char* Arg;

            void* ThreadProc() override {
                ui64 counters[NUMBER_OF_QUEUES];
                for (size_t i = 0; i < NUMBER_OF_QUEUES; ++i) {
                    counters[i] = 0;
                }

                for (int i = 0; i < 100000; ++i) {
                    size_t rnd = GetCycleCount() % NUMBER_OF_QUEUES;
                    int cookie = counters[rnd]++;
                    Queues[rnd].Push(Arg + cookie);
                }

                for (size_t i = 0; i < NUMBER_OF_QUEUES; ++i) {
                    Queues[i].Push((void*)2ULL);
                }

                return nullptr;
            }
        };

        class TPopperThread: public ISimpleThread {
        public:
            TPopperThread(TQueueType* theQueue, char* base)
                : Queue(theQueue)
                , Base(base)
            {
            }

            TQueueType* Queue;
            char* Base;

            void* ThreadProc() override {
                ui64 counters[NUMBER_OF_PUSHERS];
                for (size_t i = 0; i < NUMBER_OF_PUSHERS; ++i) {
                    counters[i] = 0;
                }

                for (size_t fin = 0; fin < NUMBER_OF_PUSHERS;) {
                    auto msg = Queue->Pop();
                    if (msg == nullptr) {
                        SpinLockPause();
                        continue;
                    }
                    if (msg == (void*)2ULL) {
                        ++fin;
                        continue;
                    }
                    ui64 shift = (char*)msg - Base;
                    auto pusherNum = shift / 200000000ULL;
                    auto msgNum = shift % 200000000ULL;

                    UNIT_ASSERT_EQUAL(counters[pusherNum], msgNum);
                    ++counters[pusherNum];
                }

                auto pmsg = Queue->Pop();
                UNIT_ASSERT_VALUES_EQUAL(pmsg, nullptr);

                return nullptr;
            }
        };

        TVector<TAutoPtr<TPopperThread>> poppers;
        TVector<TAutoPtr<TPusherThread>> pushers;

        for (size_t i = 0; i < NUMBER_OF_QUEUES; ++i) {
            poppers.emplace_back(new TPopperThread(&queue[i], (char*)&queue));
            poppers.back()->Start();
        }

        for (size_t i = 0; i < NUMBER_OF_PUSHERS; ++i) {
            pushers.emplace_back(
                new TPusherThread(queue, (char*)&queue + 200000000ULL * i));
            pushers.back()->Start();
        }

        for (size_t i = 0; i < NUMBER_OF_QUEUES; ++i) {
            poppers[i]->Join();
        }

        for (size_t i = 0; i < NUMBER_OF_PUSHERS; ++i) {
            pushers[i]->Join();
        }
    }

    void Threads8_RndPush100K_Threads8_Queues() {
        ManyRndPush100K_ManyQueues<8, 8>();
    }

    /*
    void Threads24_RndPush100K_Threads24_Queues() {
        ManyRndPush100K_ManyQueues<24, 24>();
    }

    void Threads24_RndPush100K_Threads8_Queues() {
        ManyRndPush100K_ManyQueues<24, 8>();
    }

    void Threads24_RndPush100K_Threads4_Queues() {
        ManyRndPush100K_ManyQueues<24, 4>();
    }
    */
};

REGISTER_TESTS_FOR_ALL_ORDERED_QUEUES(TQueueTestProcs);
