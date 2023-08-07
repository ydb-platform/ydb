#include "equeue.h"
#include <library/cpp/threading/equeue/fast/equeue.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/event.h>
#include <util/datetime/base.h>
#include <util/generic/vector.h>

Y_UNIT_TEST_SUITE(TElasticQueueTest) {
    const size_t MaxQueueSize = 20;
    const size_t ThreadCount = 10;

    template <typename T>
    THolder<T> MakeQueue();

    template <>
    THolder<TElasticQueue> MakeQueue() {
        return MakeHolder<TElasticQueue>(MakeHolder<TSimpleThreadPool>());
    }

    template <>
    THolder<TFastElasticQueue> MakeQueue() {
        return MakeHolder<TFastElasticQueue>();
    }

    template <typename T>
    struct TEnv {
        static inline THolder<T> Queue;

        struct TQueueSetup {
            TQueueSetup() {
                Queue.Reset(MakeQueue<T>());
                Queue->Start(ThreadCount, MaxQueueSize);
            }
            ~TQueueSetup() {
                Queue->Stop();
            }
        };
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

    template <typename T>
    void FillTest() {
        Counters.Reset();

        struct TWaitJob: public IObjectInQueue {
            void Process(void*) override {
                WaitEvent.Wait();
                AtomicIncrement(Counters.Processed);
            }
        } job;

        struct TLocalSetup: TEnv<T>::TQueueSetup {
            TLocalSetup() {
                WaitEvent.Reset();
            }
            ~TLocalSetup() {
                WaitEvent.Signal();
            }
        };

        size_t enqueued = 0;
        {
            TLocalSetup setup;
            while (TEnv<T>::Queue->Add(&job) && enqueued < MaxQueueSize + 100) {
                ++enqueued;
            }

            UNIT_ASSERT_VALUES_EQUAL(enqueued, MaxQueueSize);
            UNIT_ASSERT_VALUES_EQUAL(enqueued, TEnv<T>::Queue->ObjectCount());
        }

        UNIT_ASSERT_VALUES_EQUAL(0u, TEnv<T>::Queue->ObjectCount());
        UNIT_ASSERT_VALUES_EQUAL(0u, TEnv<T>::Queue->Size());
        UNIT_ASSERT_VALUES_EQUAL((size_t)Counters.Processed, enqueued);
    }

    Y_UNIT_TEST(FillTest) {
        FillTest<TElasticQueue>();
    }

    Y_UNIT_TEST(FillTestFast) {
        FillTest<TFastElasticQueue>();
    }

//concurrent test -- send many jobs from different threads
    struct TJob: public IObjectInQueue {
        void Process(void*) override {
            AtomicIncrement(Counters.Processed);
        }
    };
    static TJob Job;

    template <typename T>
    static bool TryAdd() {
        AtomicIncrement(Counters.Total);
        if (TEnv<T>::Queue->Add(&Job)) {
            AtomicIncrement(Counters.Scheduled);
            return true;
        } else {
            AtomicIncrement(Counters.Discarded);
            return false;
        }
    }

    const size_t N = 100000;
    static size_t TryCounter;

    template <typename T>
    void ConcurrentTest() {
        Counters.Reset();
        TryCounter = 0;

        struct TSender: public IThreadFactory::IThreadAble {
            void DoExecute() override {
                while ((size_t)AtomicIncrement(TryCounter) <= N) {
                    if (!TryAdd<T>()) {
                        Sleep(TDuration::MicroSeconds(50));
                    }
                }
            }
        } sender;

        {
            typename TEnv<T>::TQueueSetup setup;

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

    Y_UNIT_TEST(ConcurrentTest) {
        ConcurrentTest<TElasticQueue>();
    }

    Y_UNIT_TEST(ConcurrentTestFast) {
        ConcurrentTest<TFastElasticQueue>();
    }
}
