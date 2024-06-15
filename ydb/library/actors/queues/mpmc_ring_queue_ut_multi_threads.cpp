#include <unordered_map>
#define MPMC_RING_QUEUE_COLLECT_STATISTICS

#include "mpmc_ring_queue.h"
#include "bench/queue.h"
#include "bench/bench_cases.h"

#include <library/cpp/testing/unittest/registar.h>

#include <memory>
#include <util/random/random.h>
#include <util/system/thread.h>

#include <queue>


using namespace NActors;
using namespace NActors::NQueueBench;

namespace { // Tests


    struct TStatsCollector {
        using TStatsSource = NActors::TStatsObserver;

        TMutex Mutex;
        NActors::TStatsObserver::TStats Stats;
        std::vector<TConsumerInfo> ConsumerInfo;

        void AddStats(const NActors::TStatsObserver::TStats &stats) {
            TGuard<TMutex> guard(Mutex);
            Stats += stats;
        }
    };


}


#define BASIC_PUSH_POP_SINGLE_THREAD_FAST(QUEUE)                                       \
    Y_UNIT_TEST(BasicPushPopSingleThread_ ## QUEUE) {                                  \
        TStatsCollector collector = TTestCases<TMPMCRingQueueWithStats<10>, TAdaptorWithStats<QUEUE>::Type<10>>::TBasicPushPopSingleThread<TStatsCollector>().Run(); \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPush, 2);                    \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPush, 2);                \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangeFastPushToSlowPush, 0);        \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPush, 0);                \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPush, 0);                     \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedSlowPushAttempt, 0);           \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPop, 2);                      \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPop, 0);                       \
    }                                                                                  \
// end BASIC_PUSH_POP_SINGLE_THREAD


#define BASIC_PUSH_POP_SINGLE_THREAD_SLOW(QUEUE)                                       \
    Y_UNIT_TEST(BasicPushPopSingleThread_ ## QUEUE) {                                  \
        TStatsCollector collector = TTestCases<TMPMCRingQueueWithStats<10>, TAdaptorWithStats<QUEUE>::Type<10>>::TBasicPushPopSingleThread<TStatsCollector>().Run(); \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPush, 2);                    \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPush, 0);                \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangeFastPushToSlowPush, 0);        \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPush, 2);                \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPush, 0);                     \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedSlowPushAttempt, 0);           \
    }                                                                                  \
// end BASIC_PUSH_POP_SINGLE_THREAD


#define BASIC_PUSH_POP_MUTLI_THREADS_FAST(QUEUE)                                                                \
    Y_UNIT_TEST(BasicPushPopMultiThreads_ ## QUEUE) {                                                           \
        constexpr ui32 ThreadCount = 10;                                                                        \
        constexpr ui32 RepeatCount = 1000;                                                                      \
        TStatsCollector collector = TTestCases<TMPMCRingQueueWithStats<10>, TAdaptorWithStats<QUEUE>::Type<10>>::TBasicPushPopMultiThreads<TStatsCollector, ThreadCount, RepeatCount>().Run();\
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPush, RepeatCount * ThreadCount);                     \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangeFastPushToSlowPush, collector.Stats.SuccessSlowPush); \
        UNIT_ASSERT_VALUES_EQUAL(                                                                               \
                collector.Stats.SuccessFastPush + collector.Stats.SuccessSlowPush,                          \
                collector.Stats.SuccessPush                                                                   \
        );                                                                                                      \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPush, 0);                                              \
    }                                                                                                           \
// end BASIC_PUSH_POP_MUTLI_THREADS_FAST


#define BASIC_PUSH_POP_MUTLI_THREADS_SLOW(QUEUE)                                                                \
    Y_UNIT_TEST(BasicPushPopMultiThreads_ ## QUEUE) {                                                           \
        constexpr ui32 ThreadCount = 10;                                                                        \
        constexpr ui32 RepeatCount = 1000;                                                                      \
        TStatsCollector collector = TTestCases<TMPMCRingQueueWithStats<10>, TAdaptorWithStats<QUEUE>::Type<10>>::TBasicPushPopMultiThreads<TStatsCollector, ThreadCount, RepeatCount>().Run();\
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPush, RepeatCount * ThreadCount);                     \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangeFastPushToSlowPush, 0);                                 \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPush, 0);                                         \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPush, collector.Stats.SuccessPush);             \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPush, 0);                                              \
    }                                                                                                           \
// end BASIC_PUSH_POP_MUTLI_THREADS_SLOW


#define BASIC_PRODUCING_FAST(QUEUE)                                                                             \
    Y_UNIT_TEST(BasicProducing_ ## QUEUE) {                                                                     \
        constexpr ui32 SizeBits = 10;                                                                           \
        constexpr ui32 MaxSize = 1 << SizeBits;                                                                 \
        constexpr ui32 ThreadCount = 10;                                                                        \
        TStatsCollector collector = TTestCases<TMPMCRingQueueWithStats<10>, TAdaptorWithStats<QUEUE>::Type<10>>::TBasicProducing<TStatsCollector, ThreadCount>().Run();                 \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPush, MaxSize);                                       \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPush, MaxSize);                                   \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangeFastPushToSlowPush, ThreadCount);                       \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPush, 0);                                         \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPush, ThreadCount);                                    \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedSlowPushAttempt, 0);                                    \
    }                                                                                                           \
// end BASIC_PUSH_POP_MUTLI_THREADS_FAST


#define BASIC_PRODUCING_SLOW(QUEUE)                                                                             \
    Y_UNIT_TEST(BasicProducing_ ## QUEUE) {                                                                     \
        constexpr ui32 SizeBits = 10;                                                                           \
        constexpr ui32 MaxSize = 1 << SizeBits;                                                                 \
        constexpr ui32 ThreadCount = 10;                                                                        \
        TStatsCollector collector = TTestCases<TMPMCRingQueueWithStats<SizeBits>, TAdaptorWithStats<QUEUE>::Type<SizeBits>>::TBasicProducing<TStatsCollector, ThreadCount>().Run();                 \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPush, MaxSize);                                       \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangeFastPushToSlowPush, 0);                                 \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPush, 0);                                         \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPush, collector.Stats.SuccessPush);             \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPush, ThreadCount);                                    \
    }                                                                                                           \
// end BASIC_PUSH_POP_MUTLI_THREADS_SLOW


#define CONSUMING_EMPTY_QUEUE(QUEUE)                                                                            \
    Y_UNIT_TEST(ConsumingEmptyQueue_ ## QUEUE) {                                                                \
        constexpr ui32 SizeBits = 10;                                                                           \
        constexpr ui32 MaxSize = 1 << SizeBits;                                                                 \
        constexpr ui32 ThreadCount = 10;                                                                        \
        TStatsCollector collector = TTestCases<TMPMCRingQueueWithStats<SizeBits>, TAdaptorWithStats<QUEUE>::Type<SizeBits>>::TConsumingEmptyQueue<TStatsCollector, ThreadCount, MaxSize>().Run();   \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPop, ThreadCount * MaxSize);                            \
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPop, 0);                                               \
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
        TStatsCollector collector = TTestCases<TMPMCRingQueueWithStats<SizeBits>, TAdaptorWithStats<TVeryFastQueue>::Type<SizeBits>>::TConsumingEmptyQueue<TStatsCollector, ThreadCount, MaxSize>().Run();
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPop, ThreadCount * MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedReallyFastPop, ThreadCount * MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPop, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessReallyFastPop, 0);
        UNIT_ASSERT_LT(collector.Stats.FailedReallyFastPopAttempt, MaxSize); // lower than 10%
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.InvalidatedSlotInReallyFastPop, collector.Stats.FailedPop + collector.Stats.FailedReallyFastPopAttempt);
    }

    Y_UNIT_TEST(ConsumingEmptyQueue_TFastQueue) {
        return;
        constexpr ui32 SizeBits = 10;
        constexpr ui32 MaxSize = 1 << SizeBits;
        constexpr ui32 ThreadCount = 10;
        TStatsCollector collector = TTestCases<TMPMCRingQueueWithStats<SizeBits>, TAdaptorWithStats<TFastQueue>::Type<SizeBits>>::TConsumingEmptyQueue<TStatsCollector, ThreadCount, MaxSize>().Run();
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPop, ThreadCount * MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedFastPop, ThreadCount * MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPop, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPop, 0);
        UNIT_ASSERT_LT(collector.Stats.FailedReallyFastPopAttempt, MaxSize); // lower than 10%
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.InvalidatedSlotInFastPop, collector.Stats.FailedPop + collector.Stats.FailedFastPopAttempt);
    }

    Y_UNIT_TEST(ConsumingEmptyQueue_TSlowQueue) {
        constexpr ui32 SizeBits = 10;
        constexpr ui32 MaxSize = 1 << SizeBits;
        constexpr ui32 ThreadCount = 10;
        TStatsCollector collector = TTestCases<TMPMCRingQueueWithStats<SizeBits>, TAdaptorWithStats<TSlowQueue>::Type<SizeBits>>::TConsumingEmptyQueue<TStatsCollector, ThreadCount, MaxSize>().Run();
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPop, ThreadCount * MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedSlowPop, ThreadCount * MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.InvalidatedSlotInSlowPop, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPop, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPop, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedSlowPopAttempt, 0);
    }

    Y_UNIT_TEST(ConsumingEmptyQueue_TVerySlowQueue) {
        constexpr ui32 SizeBits = 10;
        constexpr ui32 MaxSize = 1 << SizeBits;
        constexpr ui32 ThreadCount = 10;
        TStatsCollector collector = TTestCases<TMPMCRingQueueWithStats<SizeBits>, TAdaptorWithStats<TVerySlowQueue>::Type<SizeBits>>::TConsumingEmptyQueue<TStatsCollector, ThreadCount, MaxSize>().Run();
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedPop, ThreadCount * MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedReallySlowPop, ThreadCount * MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedSlowPop, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangeReallySlowPopToSlowPop, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.InvalidatedSlotInSlowPop, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPop, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPop, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.FailedSlowPopAttempt, 0);
    }

    Y_UNIT_TEST(BasicProducingConsuming_TVeryFastQueue) {
        constexpr ui32 SizeBits = 15;
        constexpr ui32 ItemsPerThread = 1024;
        constexpr ui32 ThreadCount = 10;
        TStatsCollector collector = TTestCases<TMPMCRingQueueWithStats<SizeBits>, TAdaptorWithStats<TVeryFastQueue>::Type<SizeBits>>::TBasicProducingConsuming<TStatsCollector, ThreadCount, ThreadCount, ItemsPerThread, ItemsPerThread>().Run();

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPush, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPush + collector.Stats.SuccessSlowPush, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPush + collector.Stats.FailedPush, collector.Stats.ChangeFastPushToSlowPush);
        UNIT_ASSERT_LT(collector.Stats.SuccessSlowPush, collector.Stats.SuccessFastPush);

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPop, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessReallyFastPop, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.InvalidatedSlotInReallyFastPop, collector.Stats.FailedPop + collector.Stats.FailedReallyFastPopAttempt);
        UNIT_ASSERT_LT(collector.Stats.InvalidatedSlotInReallyFastPop, ItemsPerThread); // lower than 10%

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
        TStatsCollector collector = TTestCases<TMPMCRingQueueWithStats<SizeBits>, TAdaptorWithStats<TFastQueue>::Type<SizeBits>>::TBasicProducingConsuming<TStatsCollector, ThreadCount, ThreadCount, ItemsPerThread, ItemsPerThread>().Run();

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPush, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPush + collector.Stats.SuccessSlowPush, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPush + collector.Stats.FailedPush, collector.Stats.ChangeFastPushToSlowPush);
        UNIT_ASSERT_LT(collector.Stats.SuccessSlowPush, collector.Stats.SuccessFastPush);

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPop, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPop, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.InvalidatedSlotInFastPop, collector.Stats.FailedPop + collector.Stats.FailedFastPopAttempt);
        UNIT_ASSERT_LT(collector.Stats.InvalidatedSlotInFastPop, ItemsPerThread); // lower than 10%

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
        TStatsCollector collector = TTestCases<TMPMCRingQueueWithStats<SizeBits>, TAdaptorWithStats<TSlowQueue>::Type<SizeBits>>::TBasicProducingConsuming<TStatsCollector, ThreadCount, ThreadCount, ItemsPerThread, ItemsPerThread>().Run();

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPush, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPush, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPush, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangeFastPushToSlowPush, 0);

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPop, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPop, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.InvalidatedSlotInSlowPop, 0);

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
        TStatsCollector collector = TTestCases<TMPMCRingQueueWithStats<SizeBits>, TAdaptorWithStats<TVerySlowQueue>::Type<SizeBits>>::TBasicProducingConsuming<TStatsCollector, ThreadCount, ThreadCount, ItemsPerThread, ItemsPerThread>().Run();

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPush, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPush, 0);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPush, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.ChangeFastPushToSlowPush, 0);

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPop, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPop, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.InvalidatedSlotInSlowPop, 0);

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
        TStatsCollector collector = TTestCases<TMPMCRingQueueWithStats<SizeBits>, TAdaptorWithStats<TSingleQueue>::Type<SizeBits>>::TBasicProducingConsuming<TStatsCollector, ThreadCount, 1, ItemsPerThread, ThreadCount * ItemsPerThread>().Run();

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPush, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPush + collector.Stats.SuccessSlowPush, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPush + collector.Stats.FailedPush, collector.Stats.ChangeFastPushToSlowPush);
        UNIT_ASSERT_LT(collector.Stats.SuccessSlowPush, collector.Stats.SuccessFastPush);

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPop, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSingleConsumerPop, ThreadCount * ItemsPerThread);

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
        TStatsCollector collector = TTestCases<TMPMCRingQueueWithStats<SizeBits>, TAdaptorWithStats<TAdaptiveQueue>::Type<SizeBits>>::TBasicProducingConsuming<TStatsCollector, ThreadCount, ThreadCount, ItemsPerThread, ItemsPerThread>().Run();

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPush, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessFastPush + collector.Stats.SuccessSlowPush, ThreadCount * ItemsPerThread);
        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessSlowPush + collector.Stats.FailedPush, collector.Stats.ChangeFastPushToSlowPush);
        UNIT_ASSERT_LT(collector.Stats.SuccessSlowPush, collector.Stats.SuccessFastPush);

        UNIT_ASSERT_VALUES_EQUAL(collector.Stats.SuccessPop, ThreadCount * ItemsPerThread);

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
