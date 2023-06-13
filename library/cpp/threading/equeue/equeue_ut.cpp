#include "equeue.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/event.h>
#include <util/datetime/base.h>
#include <util/generic/vector.h>

Y_UNIT_TEST_SUITE(TElasticQueueTest) {
    const size_t MaxQueueSize = 20;
    const size_t ThreadCount = 10;
    const size_t N = 100000;

    static THolder<TElasticQueue> Queue;

    struct TQueueSetup {
        TQueueSetup() {
            Queue.Reset(new TElasticQueue(MakeHolder<TSimpleThreadPool>()));
            Queue->Start(ThreadCount, MaxQueueSize);
        }
        ~TQueueSetup() {
            Queue->Stop();
        }
    };

    struct TCounters {
        void Reset() {
            Processed = Scheduled = Discarded = Total = 0;
        }

        TAtomic Processed;
        TAtomic Scheduled;
        TAtomic Discarded;
        TAtomic Total;
    };
    static TCounters Counters;

//fill test -- fill queue with "endless" jobs
    TSystemEvent WaitEvent;
    Y_UNIT_TEST(FillTest) {
        Counters.Reset();

        struct TWaitJob: public IObjectInQueue {
            void Process(void*) override {
                WaitEvent.Wait();
                AtomicIncrement(Counters.Processed);
            }
        } job;

        struct TLocalSetup: TQueueSetup {
            ~TLocalSetup() {
                WaitEvent.Signal();
            }
        };

        size_t enqueued = 0;
        {
            TLocalSetup setup;
            while (Queue->Add(&job) && enqueued < MaxQueueSize + 100) {
                ++enqueued;
            }

            UNIT_ASSERT_VALUES_EQUAL(enqueued, MaxQueueSize);
            UNIT_ASSERT_VALUES_EQUAL(enqueued, Queue->ObjectCount());
        }

        UNIT_ASSERT_VALUES_EQUAL(0u, Queue->ObjectCount());
        UNIT_ASSERT_VALUES_EQUAL(0u, Queue->Size());
        UNIT_ASSERT_VALUES_EQUAL((size_t)Counters.Processed, enqueued);
    }


//concurrent test -- send many jobs from different threads
    struct TJob: public IObjectInQueue {
        void Process(void*) override {
            AtomicIncrement(Counters.Processed);
        }
    };
    static TJob Job;

    static bool TryAdd() {
        AtomicIncrement(Counters.Total);
        if (Queue->Add(&Job)) {
            AtomicIncrement(Counters.Scheduled);
            return true;
        } else {
            AtomicIncrement(Counters.Discarded);
            return false;
        }
    }

    static size_t TryCounter;

    Y_UNIT_TEST(ConcurrentTest) {
        Counters.Reset();
        TryCounter = 0;

        struct TSender: public IThreadFactory::IThreadAble {
            void DoExecute() override {
                while ((size_t)AtomicIncrement(TryCounter) <= N) {
                    if (!TryAdd()) {
                        Sleep(TDuration::MicroSeconds(50));
                    }
                }
            }
        } sender;

        {
            TQueueSetup setup;

            TVector< TAutoPtr<IThreadFactory::IThread> > senders;
            for (size_t i = 0; i < ThreadCount; ++i) {
                senders.push_back(::SystemThreadFactory()->Run(&sender));
            }

            for (size_t i = 0; i < senders.size(); ++i) {
                senders[i]->Join();
            }
        }

        UNIT_ASSERT_VALUES_EQUAL((size_t)Counters.Total, N);
        UNIT_ASSERT_VALUES_EQUAL(Counters.Processed, Counters.Scheduled);
        UNIT_ASSERT_VALUES_EQUAL(Counters.Total, Counters.Scheduled + Counters.Discarded);
    }
}
