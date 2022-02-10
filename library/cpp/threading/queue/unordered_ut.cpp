#include <library/cpp/testing/unittest/registar.h>
#include <util/system/thread.h>
#include <algorithm>
#include <util/generic/vector.h>
#include <util/random/fast.h>

#include "ut_helpers.h"

template <typename TQueueType>
class TTestUnorderedQueue: public TTestBase {
private:
    using TLink = TIntrusiveLink;

    UNIT_TEST_SUITE_DEMANGLE(TTestUnorderedQueue<TQueueType>);
    UNIT_TEST(Push1M_Pop1M_Unordered)
    UNIT_TEST_SUITE_END();

public:
    void Push1M_Pop1M_Unordered() {
        constexpr int REPEAT = 1000000;
        TQueueType queue;
        TLink msg[REPEAT];

        auto pmsg = queue.Pop();
        UNIT_ASSERT_VALUES_EQUAL(pmsg, nullptr);

        for (int i = 0; i < REPEAT; ++i) {
            queue.Push(&msg[i]);
        }

        TVector<TLink*> popped;
        popped.reserve(REPEAT);
        for (int i = 0; i < REPEAT; ++i) {
            popped.push_back((TLink*)queue.Pop());
        }

        pmsg = queue.Pop();
        UNIT_ASSERT_VALUES_EQUAL(pmsg, nullptr);

        std::sort(popped.begin(), popped.end());
        for (int i = 0; i < REPEAT; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(&msg[i], popped[i]);
        }
    }
};

template <typename TQueueType>
class TTestWeakQueue: public TTestBase {
private:
    UNIT_TEST_SUITE_DEMANGLE(TTestWeakQueue<TQueueType>);
    UNIT_TEST(Threads8_Rnd_Exchange)
    UNIT_TEST_SUITE_END();

public:
    template <ui16 COUNT = 48, ui32 MSG_COUNT = 10000>
    void ManyThreadsRndExchange() {
        TQueueType queues[COUNT];

        class TWorker: public ISimpleThread {
        public:
            TWorker(
                TQueueType* queues_,
                ui16 mine,
                TAtomic* pushDone)
                : Queues(queues_)
                , MineQueue(mine)
                , PushDone(pushDone)
            {
            }

            TQueueType* Queues;
            ui16 MineQueue;
            TVector<uintptr_t> Received;
            TAtomic* PushDone;

            void* ThreadProc() override {
                TReallyFastRng32 rng(GetCycleCount());
                Received.reserve(MSG_COUNT * 2);

                for (ui32 loop = 1; loop <= MSG_COUNT; ++loop) {
                    for (;;) {
                        auto msg = Queues[MineQueue].Pop();
                        if (msg == nullptr) {
                            break;
                        }

                        Received.push_back((uintptr_t)msg);
                    }

                    ui16 rnd = rng.GenRand64() % COUNT;
                    ui64 msg = ((ui64)MineQueue << 32) + loop;
                    while (!Queues[rnd].Push((void*)msg)) {
                    }
                }

                AtomicIncrement(*PushDone);

                for (;;) {
                    bool isItLast = AtomicGet(*PushDone) == COUNT;
                    auto msg = Queues[MineQueue].Pop();
                    if (msg != nullptr) {
                        Received.push_back((uintptr_t)msg);
                    } else {
                        if (isItLast) {
                            break;
                        }
                        SpinLockPause();
                    }
                }

                for (ui64 last = 0;;) {
                    auto msg = Queues[MineQueue].UnsafeScanningPop(&last);
                    if (msg == nullptr) {
                        break;
                    }
                    Received.push_back((uintptr_t)msg);
                }

                return nullptr;
            }
        };

        TVector<TAutoPtr<TWorker>> workers;
        TAtomic pushDone = 0;

        for (ui32 i = 0; i < COUNT; ++i) {
            workers.emplace_back(new TWorker(&queues[0], i, &pushDone));
            workers.back()->Start();
        }

        TVector<uintptr_t> all;
        for (ui32 i = 0; i < COUNT; ++i) {
            workers[i]->Join();
            all.insert(all.begin(),
                       workers[i]->Received.begin(), workers[i]->Received.end());
        }

        std::sort(all.begin(), all.end());
        auto iter = all.begin();
        for (ui32 i = 0; i < COUNT; ++i) {
            for (ui32 k = 1; k <= MSG_COUNT; ++k) {
                UNIT_ASSERT_VALUES_EQUAL(((ui64)i << 32) + k, *iter);
                ++iter;
            }
        }
    }

    void Threads8_Rnd_Exchange() {
        ManyThreadsRndExchange<8>();
    }
};

REGISTER_TESTS_FOR_ALL_UNORDERED_QUEUES(TTestUnorderedQueue);
UNIT_TEST_SUITE_REGISTRATION(TTestWeakQueue<TMPMCUnorderedRing>);
