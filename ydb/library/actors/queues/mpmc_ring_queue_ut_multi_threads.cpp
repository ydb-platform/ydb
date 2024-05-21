#include <unordered_map>
#define MPMC_RING_QUEUE_COLLECT_STATISTICS

#include "mpmc_ring_queue.h"
#include "mpmc_ring_queue_ut_base.h"

#include <library/cpp/testing/unittest/registar.h>

#include <memory>
#include <util/random/random.h>
#include <util/system/thread.h>

#include <queue>


using namespace NActors;
using namespace NActors::NTests;

namespace { // Tests

    enum class EThreadAction {
        Continue,
        Sleep,
        Kill,
    };

    struct TThreadAction {
        EThreadAction Action;
        ui64 SleepNs = 0;
    };

    enum class EWorkerAction {
        Push,
        Pop,
        Sleep,
        Kill,
    };

    enum class EExpectedStatus {
        Nothing,
        Success,
        Failure,
        RepeatUntilSuccess,
    };

    struct TWorkerAction {
        EWorkerAction Action;
        EExpectedStatus Expected = EExpectedStatus::Nothing;
        std::optional<ui64> Value;

        explicit operator TThreadAction() const {
            switch (Action) {
            case EWorkerAction::Push:
            case EWorkerAction::Pop:
                return {.Action=EThreadAction::Continue};
            case EWorkerAction::Sleep:
                UNIT_ASSERT(Value);
                return {.Action=EThreadAction::Sleep, .SleepNs=*Value};
            case EWorkerAction::Kill:
                return {.Action=EThreadAction::Kill};
            }
        }
    };

    template <typename TQueue>
    class TSimpleWorker {
    public:
        TSimpleWorker(TQueue *queue, const std::vector<TWorkerAction> actions, ui32 repeatCount = 1)
            : Queue(queue)
            , Actions(actions)
            , RepeatCount(repeatCount)
        {
            UNIT_ASSERT(actions.size());
        }

        TThreadAction Do() {
            if (Idx == Actions.size()) {
                Idx = 0;
                if (--RepeatCount == 0) {
                    return {.Action=EThreadAction::Kill};
                }
            }
            TWorkerAction &action = Actions[Idx++];
            switch (action.Action) {
            case EWorkerAction::Push: {
                    UNIT_ASSERT(action.Value);
                    bool success = Queue->TryPush(*action.Value);
                    if (action.Expected == EExpectedStatus::RepeatUntilSuccess) {
                        while (!success) {
                            success = Queue->TryPush(*action.Value);
                        }
                    } else if (action.Expected != EExpectedStatus::Nothing) {
                        UNIT_ASSERT_VALUES_EQUAL(success, (action.Expected == EExpectedStatus::Success));
                    }
                }
                break;
            case EWorkerAction::Pop: {
                    auto value = Queue->TryPop();
                    if (action.Expected == EExpectedStatus::RepeatUntilSuccess) {
                        while (!value) {
                            value = Queue->TryPop();
                        }
                    } else if (action.Expected != EExpectedStatus::Nothing) {
                        UNIT_ASSERT_VALUES_EQUAL(bool(value), (action.Expected == EExpectedStatus::Success));
                        if (value && action.Value) {
                            UNIT_ASSERT_VALUES_EQUAL(value, action.Value);
                        }
                    }
                }
                break;
            default:
                break;
            }
            return static_cast<TThreadAction>(action);
        }

    private:
        TQueue *Queue;
        std::vector<TWorkerAction> Actions;
        ui32 Idx = 0;
        ui32 RepeatCount = 0;
    };

    template <typename TQueue>
    class TProducerWorker {
    public:
        TProducerWorker(TQueue *queue, std::optional<ui32> writes)
            : Queue(queue)
            , Writes(writes)
        {}

        TThreadAction Do() {
            if (Writes) {
                if (Written == *Writes) {
                    return {.Action=EThreadAction::Kill};
                }
                auto value = Queue->TryPush(Written);
                if (value) {
                    Written++;
                }
            } else {
                auto success = Queue->TryPush(Written);
                if (!success) {
                    return {.Action=EThreadAction::Kill};
                }
                Written++;
            }
            return {.Action=EThreadAction::Continue};
        }

        TQueue *Queue;
        std::optional<ui32> Writes;
        ui64 Written = 0;
    };

    struct TConsumerInfo {
        std::vector<std::optional<ui32>> ReadedItems;
    };

    template <typename TQueue>
    class TConsumerWorker {
    public:
        TConsumerWorker(TQueue *queue, std::optional<ui32> reads, bool countFailsAsReads=false, TConsumerInfo *info=nullptr)
            : Queue(queue)
            , Reads(reads)
            , CountFailsAsReads(countFailsAsReads)
            , Info(info)
        {}

        TThreadAction Do() {
            if (Reads) {
                if (Readed == *Reads) {
                    return {.Action=EThreadAction::Kill};
                }
                auto value = Queue->TryPop();
                if (value || CountFailsAsReads) {
                    Readed++;
                    if (Info) {
                        Info->ReadedItems.emplace_back(std::move(value));
                    }
                }
            } else {
                auto value = Queue->TryPop();
                if (!value) {
                    return {.Action=EThreadAction::Kill};
                }
                Readed++;
                if (Info) {
                    Info->ReadedItems.emplace_back(std::move(value));
                }
            }
            return {.Action=EThreadAction::Continue};
        }

        TQueue *Queue;
        std::optional<ui32> Reads;
        ui64 Readed = 0;
        bool CountFailsAsReads;
        TConsumerInfo *Info;
    };

    struct TStatsCollector {
        TMutex Mutex;
        NActors::TMPMCRingQueueStats::TStats Stats;
        std::vector<TConsumerInfo> ConsumerInfo;

        void AddStats(const NActors::TMPMCRingQueueStats::TStats &stats) {
            TGuard<TMutex> guard(Mutex);
            Stats += stats;
        }
    };

    template <typename TWorker>
    class TTestThread : public ISimpleThread {
    public:
        TTestThread(TWorker worker, TStatsCollector *statsCollector = nullptr)
            : Worker(worker)
            , StatsCollector(statsCollector)
        {}

        ~TTestThread() = default;

    private:
        bool Process(const TThreadAction &action) {
            switch (action.Action) {
            case EThreadAction::Continue:
                break;
            case EThreadAction::Sleep:
                NanoSleep(action.SleepNs);
                break;
            case EThreadAction::Kill:
                if (StatsCollector) {
                    auto stats = NActors::TMPMCRingQueueStats::GetLocalStats();
                    // Cerr << (TStringBuilder() << "thread: " << (ui64)this << " pushes: " << stats.SuccessPushes.load() << Endl);
                    StatsCollector->AddStats(stats);
                }
                return true;
            }
            return false;
        }

        void* ThreadProc() final {
            for (;;) {
                TThreadAction action = Worker.Do();
                if (Process(action)) {
                    break;
                }
            }
            return nullptr;
        }

    private:
        TWorker Worker;
        TStatsCollector *StatsCollector;
    };

    void RunThreads(const std::vector<ISimpleThread*> &threads) {
        for (auto &thread : threads) {
            thread->Start();
        }
        for (auto &thread : threads) {
            thread->Join();
        }
    }

    void RunThreads(const std::vector<std::unique_ptr<ISimpleThread>> &threads) {
        for (auto &thread : threads) {
            thread->Start();
        }
        for (auto &thread : threads) {
            thread->Join();
        }
    }

    template <typename ...TThreads>
    void RunThreads(std::unique_ptr<TThreads>&& ...threads) {
        RunThreads(std::vector<ISimpleThread*>{threads.release()...});
    }


    template <ui32 SizeBits>
    struct TTestCases {

        template <template <ui32> typename TQueueAdaptor>
        static TStatsCollector BasicPushPopSingleThread() {
            TMPMCRingQueue<SizeBits> realQueue;
            TQueueAdaptor<SizeBits> adapter(&realQueue);
            TStatsCollector collector;
            TSimpleWorker<decltype(adapter)> worker(
                &adapter,
                {
                    TWorkerAction{.Action=EWorkerAction::Push, .Expected=EExpectedStatus::Success, .Value=1},
                    TWorkerAction{.Action=EWorkerAction::Push, .Expected=EExpectedStatus::Success, .Value=2},
                    TWorkerAction{.Action=EWorkerAction::Pop, .Expected=EExpectedStatus::Success, .Value=1},
                    TWorkerAction{.Action=EWorkerAction::Pop, .Expected=EExpectedStatus::Success, .Value=2},
                }
            );
            RunThreads(std::make_unique<TTestThread<decltype(worker)>>(worker, &collector));
            return std::move(collector);
        }

        template <template <ui32> typename TQueueAdaptor, ui32 ThreadCount, ui32 RepeatCount>
        static TStatsCollector BasicPushPopMultiThreads() {
            TMPMCRingQueue<SizeBits> realQueue;
            TVector<std::unique_ptr<IQueue>> adapters;
            TVector<std::unique_ptr<ISimpleThread>> threads;
            TStatsCollector collector;
            for (ui32 threadIdx = 0; threadIdx < ThreadCount; ++threadIdx) {
                TQueueAdaptor<SizeBits> *adapter = new TQueueAdaptor<SizeBits>(&realQueue);
                adapters.emplace_back(adapter);
                TSimpleWorker<std::decay_t<decltype(*adapter)>> worker(
                    adapter,
                    {
                        TWorkerAction{.Action=EWorkerAction::Push, .Expected=EExpectedStatus::Success, .Value=1},
                        TWorkerAction{.Action=EWorkerAction::Pop, .Expected=EExpectedStatus::Success, .Value=1},
                    },
                    RepeatCount
                );
                threads.emplace_back(new TTestThread<decltype(worker)>(worker, &collector));
            }
            RunThreads(threads);
            return std::move(collector);
        }

        template <template <ui32> typename TQueueAdaptor, ui32 ThreadCount>
        static TStatsCollector BasicProducing() {
            TMPMCRingQueue<SizeBits> realQueue;
            TVector<std::unique_ptr<IQueue>> adapters;
            TVector<std::unique_ptr<ISimpleThread>> threads;
            TStatsCollector collector;
            for (ui32 threadIdx = 0; threadIdx < ThreadCount; ++threadIdx) {
                TQueueAdaptor<SizeBits> *adapter = new TQueueAdaptor<SizeBits>(&realQueue);
                adapters.emplace_back(adapter);
                TProducerWorker<std::decay_t<decltype(*adapter)>> worker(adapter, {});
                threads.emplace_back(new TTestThread<decltype(worker)>(worker, &collector));
            }
            RunThreads(threads);
            return std::move(collector);
        }

        template <template <ui32> typename TQueueAdaptor, ui32 ThreadCount, ui32 Reads>
        static TStatsCollector ConsumingEmptyQueue() {
            TMPMCRingQueue<SizeBits> realQueue;
            TVector<std::unique_ptr<IQueue>> adapters;
            TVector<std::unique_ptr<ISimpleThread>> threads;
            TStatsCollector collector;
            for (ui32 threadIdx = 0; threadIdx < ThreadCount; ++threadIdx) {
                TQueueAdaptor<SizeBits> *adapter = new TQueueAdaptor<SizeBits>(&realQueue);
                adapters.emplace_back(adapter);
                TConsumerWorker<std::decay_t<decltype(*adapter)>> worker(adapter, Reads, true);
                threads.emplace_back(new TTestThread<decltype(worker)>(worker, &collector));
            }
            RunThreads(threads);
            return std::move(collector);
        }

        template <template <ui32> typename TQueueAdaptor, ui32 ProducingThreadCount, ui32 ConsumingThreadCount, ui32 PushedItems, ui32 PoppedItems>
        static TStatsCollector BasicProducingConsuming() {
            TMPMCRingQueue<SizeBits> realQueue;
            TVector<std::unique_ptr<IQueue>> adapters;
            TVector<std::unique_ptr<ISimpleThread>> threads;
            TStatsCollector collector;
            for (ui32 threadIdx = 0; threadIdx < ProducingThreadCount; ++threadIdx) {
                TQueueAdaptor<SizeBits> *adapter = new TQueueAdaptor<SizeBits>(&realQueue);
                adapters.emplace_back(adapter);
                TProducerWorker<std::decay_t<decltype(*adapter)>> worker(adapter, PushedItems);
                threads.emplace_back(new TTestThread<decltype(worker)>(worker, &collector));
            }
            collector.ConsumerInfo.resize(ConsumingThreadCount);
            for (ui32 threadIdx = 0; threadIdx < ConsumingThreadCount; ++threadIdx) {
                TQueueAdaptor<SizeBits> *adapter = new TQueueAdaptor<SizeBits>(&realQueue);
                adapters.emplace_back(adapter);
                TConsumerWorker<std::decay_t<decltype(*adapter)>> worker(adapter, PoppedItems, false, &collector.ConsumerInfo[threadIdx]);
                threads.emplace_back(new TTestThread<decltype(worker)>(worker, &collector));
            }
            RunThreads(threads);
            return std::move(collector);
        }

    };

}


#define BASIC_PUSH_POP_SINGLE_THREAD_FAST(QUEUE)                                       \
    Y_UNIT_TEST(BasicPushPopSingleThread_ ## QUEUE) {                                  \
        TStatsCollector collector = TTestCases<10>::BasicPushPopSingleThread<QUEUE>(); \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPushes, 2);                    \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPushes, 2);                \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangesFastPushToSlowPush, 0);        \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPushes, 0);                \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPushes, 0);                     \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedSlowPushAttempts, 0);           \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPops, 2);                      \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPops, 0);                       \
    }                                                                                  \
// end BASIC_PUSH_POP_SINGLE_THREAD


#define BASIC_PUSH_POP_SINGLE_THREAD_SLOW(QUEUE)                                       \
    Y_UNIT_TEST(BasicPushPopSingleThread_ ## QUEUE) {                                  \
        TStatsCollector collector = TTestCases<10>::BasicPushPopSingleThread<QUEUE>(); \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPushes, 2);                    \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPushes, 0);                \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangesFastPushToSlowPush, 0);        \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPushes, 2);                \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPushes, 0);                     \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedSlowPushAttempts, 0);           \
    }                                                                                  \
// end BASIC_PUSH_POP_SINGLE_THREAD


#define BASIC_PUSH_POP_MUTLI_THREADS_FAST(QUEUE)                                                                \
    Y_UNIT_TEST(BasicPushPopMultiThreads_ ## QUEUE) {                                                           \
        constexpr ui32 ThreadCount = 10;                                                                        \
        constexpr ui32 RepeatCount = 1000;                                                                      \
        TStatsCollector collector = TTestCases<10>::BasicPushPopMultiThreads<QUEUE, ThreadCount, RepeatCount>();\
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPushes, RepeatCount * ThreadCount);                     \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangesFastPushToSlowPush, collector.Stats.SuccessSlowPushes); \
        UNIT_ASSERT_VALUES_EQUAL(                                                                               \
                collector.Stats.SuccessFastPushes + collector.Stats.SuccessSlowPushes,                          \
                collector.Stats.SuccessPushes                                                                   \
        );                                                                                                      \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPushes, 0);                                              \
    }                                                                                                           \
// end BASIC_PUSH_POP_MUTLI_THREADS_FAST


#define BASIC_PUSH_POP_MUTLI_THREADS_SLOW(QUEUE)                                                                \
    Y_UNIT_TEST(BasicPushPopMultiThreads_ ## QUEUE) {                                                           \
        constexpr ui32 ThreadCount = 10;                                                                        \
        constexpr ui32 RepeatCount = 1000;                                                                      \
        TStatsCollector collector = TTestCases<10>::BasicPushPopMultiThreads<QUEUE, ThreadCount, RepeatCount>();\
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPushes, RepeatCount * ThreadCount);                     \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangesFastPushToSlowPush, 0);                                 \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPushes, 0);                                         \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPushes, collector.Stats.SuccessPushes);             \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPushes, 0);                                              \
    }                                                                                                           \
// end BASIC_PUSH_POP_MUTLI_THREADS_SLOW


#define BASIC_PRODUCING_FAST(QUEUE)                                                                             \
    Y_UNIT_TEST(BasicProducing_ ## QUEUE) {                                                                     \
        constexpr ui32 SizeBits = 10;                                                                           \
        constexpr ui32 MaxSize = 1 << SizeBits;                                                                 \
        constexpr ui32 ThreadCount = 10;                                                                        \
        TStatsCollector collector = TTestCases<SizeBits>::BasicProducing<QUEUE, ThreadCount>();                 \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPushes, MaxSize);                                       \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPushes, MaxSize);                                   \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangesFastPushToSlowPush, ThreadCount);                       \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPushes, 0);                                         \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPushes, ThreadCount);                                    \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedSlowPushAttempts, 0);                                    \
    }                                                                                                           \
// end BASIC_PUSH_POP_MUTLI_THREADS_FAST


#define BASIC_PRODUCING_SLOW(QUEUE)                                                                             \
    Y_UNIT_TEST(BasicProducing_ ## QUEUE) {                                                                     \
        constexpr ui32 SizeBits = 10;                                                                           \
        constexpr ui32 MaxSize = 1 << SizeBits;                                                                 \
        constexpr ui32 ThreadCount = 10;                                                                        \
        TStatsCollector collector = TTestCases<SizeBits>::BasicProducing<QUEUE, ThreadCount>();                 \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPushes, MaxSize);                                       \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangesFastPushToSlowPush, 0);                                 \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPushes, 0);                                         \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPushes, collector.Stats.SuccessPushes);             \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPushes, ThreadCount);                                    \
    }                                                                                                           \
// end BASIC_PUSH_POP_MUTLI_THREADS_SLOW


#define CONSUMING_EMPTY_QUEUE(QUEUE)                                                                            \
    Y_UNIT_TEST(ConsumingEmptyQueue_ ## QUEUE) {                                                                \
        constexpr ui32 SizeBits = 10;                                                                           \
        constexpr ui32 MaxSize = 1 << SizeBits;                                                                 \
        constexpr ui32 ThreadCount = 10;                                                                        \
        TStatsCollector collector = TTestCases<SizeBits>::ConsumingEmptyQueue<QUEUE, ThreadCount, MaxSize>();   \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPops, ThreadCount * MaxSize);                            \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPops, 0);                                               \
    }                                                                                                           \
// end BASIC_PUSH_POP_MUTLI_THREADS_SLOW


Y_UNIT_TEST_SUITE(MPMCRingQueueMultiThreadsTests) {

    BASIC_PUSH_POP_SINGLE_THREAD_FAST(TVeryFastQueue)
    BASIC_PUSH_POP_SINGLE_THREAD_FAST(TFastQueue)
    BASIC_PUSH_POP_SINGLE_THREAD_SLOW(TSlowQueue)
    BASIC_PUSH_POP_SINGLE_THREAD_SLOW(TVerySlowQueue)

    BASIC_PUSH_POP_MUTLI_THREADS_FAST(TVeryFastQueue)
    BASIC_PUSH_POP_MUTLI_THREADS_FAST(TFastQueue)
    BASIC_PUSH_POP_MUTLI_THREADS_SLOW(TSlowQueue)
    BASIC_PUSH_POP_MUTLI_THREADS_SLOW(TVerySlowQueue)

    BASIC_PRODUCING_FAST(TVeryFastQueue)
    BASIC_PRODUCING_FAST(TFastQueue)
    BASIC_PRODUCING_SLOW(TSlowQueue)
    BASIC_PRODUCING_SLOW(TVerySlowQueue)

    Y_UNIT_TEST(ConsumingEmptyQueue_TVeryFastQueue) {
        return;
        constexpr ui32 SizeBits = 10;
        constexpr ui32 MaxSize = 1 << SizeBits;
        constexpr ui32 ThreadCount = 10;
        TStatsCollector collector = TTestCases<SizeBits>::ConsumingEmptyQueue<TVeryFastQueue, ThreadCount, MaxSize>();
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPops, ThreadCount * MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedReallyFastPops, ThreadCount * MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPops, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessReallyFastPops, 0);
        UNIT_ASSERT_LT(collector.Stats.FailedReallyFastPopAttempts, MaxSize); // lower than 10%
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.InvalidatedSlotsInReallyFastPop, collector.Stats.FailedPops + collector.Stats.FailedReallyFastPopAttempts);
    }

    Y_UNIT_TEST(ConsumingEmptyQueue_TFastQueue) {
        return;
        constexpr ui32 SizeBits = 10;
        constexpr ui32 MaxSize = 1 << SizeBits;
        constexpr ui32 ThreadCount = 10;
        TStatsCollector collector = TTestCases<SizeBits>::ConsumingEmptyQueue<TFastQueue, ThreadCount, MaxSize>();
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPops, ThreadCount * MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedFastPops, ThreadCount * MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPops, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPops, 0);
        UNIT_ASSERT_LT(collector.Stats.FailedReallyFastPopAttempts, MaxSize); // lower than 10%
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.InvalidatedSlotsInFastPop, collector.Stats.FailedPops + collector.Stats.FailedFastPopAttempts);
    }

    Y_UNIT_TEST(ConsumingEmptyQueue_TSlowQueue) {
        constexpr ui32 SizeBits = 10;
        constexpr ui32 MaxSize = 1 << SizeBits;
        constexpr ui32 ThreadCount = 10;
        TStatsCollector collector = TTestCases<SizeBits>::ConsumingEmptyQueue<TSlowQueue, ThreadCount, MaxSize>();
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPops, ThreadCount * MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedSlowPops, ThreadCount * MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.InvalidatedSlotsInSlowPop, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPops, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPops, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedSlowPopAttempts, 0);
    }

    Y_UNIT_TEST(ConsumingEmptyQueue_TVerySlowQueue) {
        constexpr ui32 SizeBits = 10;
        constexpr ui32 MaxSize = 1 << SizeBits;
        constexpr ui32 ThreadCount = 10;
        TStatsCollector collector = TTestCases<SizeBits>::ConsumingEmptyQueue<TVerySlowQueue, ThreadCount, MaxSize>();
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPops, ThreadCount * MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedReallySlowPops, ThreadCount * MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedSlowPops, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangesReallySlowPopToSlowPop, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.InvalidatedSlotsInSlowPop, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPops, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPops, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedSlowPopAttempts, 0);
    }

    Y_UNIT_TEST(BasicProducingConsuming_TVeryFastQueue) {
        constexpr ui32 SizeBits = 15;
        constexpr ui32 ItemsPerThread = 1024;
        constexpr ui32 ThreadCount = 10;
        TStatsCollector collector = TTestCases<SizeBits>::BasicProducingConsuming<TVeryFastQueue, ThreadCount, ThreadCount, ItemsPerThread, ItemsPerThread>();

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPushes, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPushes + collector.Stats.SuccessSlowPushes, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPushes + collector.Stats.FailedPushes, collector.Stats.ChangesFastPushToSlowPush);
        UNIT_ASSERT_LT(collector.Stats.SuccessSlowPushes, collector.Stats.SuccessFastPushes);

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPops, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessReallyFastPops, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.InvalidatedSlotsInReallyFastPop, collector.Stats.FailedPops + collector.Stats.FailedReallyFastPopAttempts);
        UNIT_ASSERT_LT(collector.Stats.InvalidatedSlotsInReallyFastPop, ItemsPerThread); // lower than 10%

        std::unordered_map<ui32, ui32> itemsCounts;
        for (auto &info : collector.ConsumerInfo) {
            for (std::optional<ui32> item : info.ReadedItems) {
                UNIT_ASSERT(item);
                itemsCounts[*item]++;
            }
        }

        for (auto &[item, count] : itemsCounts) {
            UNIT_ASSERT_VALUES_EQUAL(count, 10);
            UNIT_ASSERT_LE(0, item);
            UNIT_ASSERT_LT(item, ItemsPerThread);
        }
        UNIT_ASSERT_VALUES_EQUAL(itemsCounts.size(), ItemsPerThread);
    }

    Y_UNIT_TEST(BasicProducingConsuming_TFastQueue) {
        constexpr ui32 SizeBits = 15;
        constexpr ui32 ItemsPerThread = 1024;
        constexpr ui32 ThreadCount = 10;
        TStatsCollector collector = TTestCases<SizeBits>::BasicProducingConsuming<TFastQueue, ThreadCount, ThreadCount, ItemsPerThread, ItemsPerThread>();

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPushes, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPushes + collector.Stats.SuccessSlowPushes, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPushes + collector.Stats.FailedPushes, collector.Stats.ChangesFastPushToSlowPush);
        UNIT_ASSERT_LT(collector.Stats.SuccessSlowPushes, collector.Stats.SuccessFastPushes);

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPops, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPops, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.InvalidatedSlotsInFastPop, collector.Stats.FailedPops + collector.Stats.FailedFastPopAttempts);
        UNIT_ASSERT_LT(collector.Stats.InvalidatedSlotsInFastPop, ItemsPerThread); // lower than 10%

        std::unordered_map<ui32, ui32> itemsCounts;
        for (auto &info : collector.ConsumerInfo) {
            for (std::optional<ui32> item : info.ReadedItems) {
                UNIT_ASSERT(item);
                itemsCounts[*item]++;
            }
        }

        for (auto &[item, count] : itemsCounts) {
            UNIT_ASSERT_VALUES_EQUAL(count, 10);
            UNIT_ASSERT_LE(0, item);
            UNIT_ASSERT_LT(item, ItemsPerThread);
        }
        UNIT_ASSERT_VALUES_EQUAL(itemsCounts.size(), ItemsPerThread);
    }

    Y_UNIT_TEST(BasicProducingConsuming_TSlowQueue) {
        constexpr ui32 SizeBits = 15;
        constexpr ui32 ItemsPerThread = 1024;
        constexpr ui32 ThreadCount = 10;
        TStatsCollector collector = TTestCases<SizeBits>::BasicProducingConsuming<TSlowQueue, ThreadCount, ThreadCount, ItemsPerThread, ItemsPerThread>();

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPushes, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPushes, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPushes, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangesFastPushToSlowPush, 0);

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPops, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPops, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.InvalidatedSlotsInSlowPop, 0);

        std::unordered_map<ui32, ui32> itemsCounts;
        for (auto &info : collector.ConsumerInfo) {
            for (std::optional<ui32> item : info.ReadedItems) {
                UNIT_ASSERT(item);
                itemsCounts[*item]++;
            }
        }

        for (auto &[item, count] : itemsCounts) {
            UNIT_ASSERT_VALUES_EQUAL(count, 10);
            UNIT_ASSERT_LE(0, item);
            UNIT_ASSERT_LT(item, ItemsPerThread);
        }
        UNIT_ASSERT_VALUES_EQUAL(itemsCounts.size(), ItemsPerThread);
    }

    Y_UNIT_TEST(BasicProducingConsuming_TVerySlowQueue) {
        constexpr ui32 SizeBits = 15;
        constexpr ui32 ItemsPerThread = 1024;
        constexpr ui32 ThreadCount = 10;
        TStatsCollector collector = TTestCases<SizeBits>::BasicProducingConsuming<TVerySlowQueue, ThreadCount, ThreadCount, ItemsPerThread, ItemsPerThread>();

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPushes, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPushes, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPushes, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangesFastPushToSlowPush, 0);

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPops, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPops, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.InvalidatedSlotsInSlowPop, 0);

        std::unordered_map<ui32, ui32> itemsCounts;
        for (auto &info : collector.ConsumerInfo) {
            for (std::optional<ui32> item : info.ReadedItems) {
                UNIT_ASSERT(item);
                itemsCounts[*item]++;
            }
        }

        for (auto &[item, count] : itemsCounts) {
            UNIT_ASSERT_VALUES_EQUAL(count, 10);
            UNIT_ASSERT_LE(0, item);
            UNIT_ASSERT_LT(item, ItemsPerThread);
        }
        UNIT_ASSERT_VALUES_EQUAL(itemsCounts.size(), ItemsPerThread);
    }

    Y_UNIT_TEST(BasicProducingConsuming_TSingleQueue) {
        constexpr ui32 SizeBits = 15;
        constexpr ui32 ItemsPerThread = 1024;
        constexpr ui32 ThreadCount = 10;
        TStatsCollector collector = TTestCases<SizeBits>::BasicProducingConsuming<TSingleQueue, ThreadCount, 1, ItemsPerThread, ThreadCount * ItemsPerThread>();

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPushes, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPushes + collector.Stats.SuccessSlowPushes, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPushes + collector.Stats.FailedPushes, collector.Stats.ChangesFastPushToSlowPush);
        UNIT_ASSERT_LT(collector.Stats.SuccessSlowPushes, collector.Stats.SuccessFastPushes);

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPops, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSingleConsumerPops, ThreadCount * ItemsPerThread);

        std::unordered_map<ui32, ui32> itemsCounts;
        for (auto &info : collector.ConsumerInfo) {
            for (std::optional<ui32> item : info.ReadedItems) {
                UNIT_ASSERT(item);
                itemsCounts[*item]++;
            }
        }

        for (auto &[item, count] : itemsCounts) {
            UNIT_ASSERT_VALUES_EQUAL(count, 10);
            UNIT_ASSERT_LE(0, item);
            UNIT_ASSERT_LT(item, ItemsPerThread);
        }
        UNIT_ASSERT_VALUES_EQUAL(itemsCounts.size(), ItemsPerThread);
    }

    Y_UNIT_TEST(BasicProducingConsuming_TAdaptiveQueue) {
        constexpr ui32 SizeBits = 15;
        constexpr ui32 ItemsPerThread = 1024;
        constexpr ui32 ThreadCount = 10;
        TStatsCollector collector = TTestCases<SizeBits>::BasicProducingConsuming<TAdaptiveQueue, ThreadCount, ThreadCount, ItemsPerThread, ItemsPerThread>();

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPushes, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPushes + collector.Stats.SuccessSlowPushes, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPushes + collector.Stats.FailedPushes, collector.Stats.ChangesFastPushToSlowPush);
        UNIT_ASSERT_LT(collector.Stats.SuccessSlowPushes, collector.Stats.SuccessFastPushes);

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPops, ThreadCount * ItemsPerThread);

        std::unordered_map<ui32, ui32> itemsCounts;
        for (auto &info : collector.ConsumerInfo) {
            for (std::optional<ui32> item : info.ReadedItems) {
                UNIT_ASSERT(item);
                itemsCounts[*item]++;
            }
        }

        for (auto &[item, count] : itemsCounts) {
            UNIT_ASSERT_VALUES_EQUAL(count, 10);
            UNIT_ASSERT_LE(0, item);
            UNIT_ASSERT_LT(item, ItemsPerThread);
        }
        UNIT_ASSERT_VALUES_EQUAL(itemsCounts.size(), ItemsPerThread);
    }

}
