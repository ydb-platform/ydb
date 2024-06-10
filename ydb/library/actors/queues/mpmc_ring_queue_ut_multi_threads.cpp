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
        TMutex Mutex;
        NActors::TMPMCRingQueueStats::TStats Stats;
        std::vector<TConsumerInfo> ConsumerInfo;

        void AddStats(const NActors::TMPMCRingQueueStats::TStats &stats) {
            TGuard<TMutex> guard(Mutex);
            Stats += stats;
        }
    };


}


#define BASIC_PUSH_POP_SINGLE_THREAD_FAST(QUEUE)                                       \
    Y_UNIT_TEST(BasicPushPopSingleThread_ ## QUEUE) {                                  \
        TStatsCollector collector = TBenchCases<TMPMCRingQueue<10>, QUEUE<10>>::BasicPushPopSingleThread<TStatsCollector>(); \
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
        TStatsCollector collector = TBenchCases<TMPMCRingQueue<10>, QUEUE<10>>::BasicPushPopSingleThread<TStatsCollector>(); \
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
        TStatsCollector collector = TBenchCases<TMPMCRingQueue<10>, QUEUE<10>>::BasicPushPopMultiThreads<TStatsCollector, ThreadCount, RepeatCount>();\
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
        TStatsCollector collector = TBenchCases<TMPMCRingQueue<10>, QUEUE<10>>::BasicPushPopMultiThreads<TStatsCollector, ThreadCount, RepeatCount>();\
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
        TStatsCollector collector = TBenchCases<TMPMCRingQueue<SizeBits>, QUEUE<SizeBits>>::BasicProducing<TStatsCollector, ThreadCount>();                 \
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
        TStatsCollector collector = TBenchCases<TMPMCRingQueue<SizeBits>, QUEUE<SizeBits>>::BasicProducing<TStatsCollector, ThreadCount>();                 \
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
        TStatsCollector collector = TBenchCases<TMPMCRingQueue<SizeBits>, QUEUE<SizeBits>>::ConsumingEmptyQueue<TStatsCollector, ThreadCount, MaxSize>();   \
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
        TStatsCollector collector = TBenchCases<TMPMCRingQueue<SizeBits>, TVeryFastQueue<SizeBits>>::ConsumingEmptyQueue<TStatsCollector, ThreadCount, MaxSize>();
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
        TStatsCollector collector = TBenchCases<TMPMCRingQueue<SizeBits>, TFastQueue<SizeBits>>::ConsumingEmptyQueue<TStatsCollector, ThreadCount, MaxSize>();
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
        TStatsCollector collector = TBenchCases<TMPMCRingQueue<SizeBits>, TSlowQueue<SizeBits>>::ConsumingEmptyQueue<TStatsCollector, ThreadCount, MaxSize>();
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
        TStatsCollector collector = TBenchCases<TMPMCRingQueue<SizeBits>, TVerySlowQueue<SizeBits>>::ConsumingEmptyQueue<TStatsCollector, ThreadCount, MaxSize>();
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
        TStatsCollector collector = TBenchCases<TMPMCRingQueue<SizeBits>, TVeryFastQueue<SizeBits>>::BasicProducingConsuming<TStatsCollector, ThreadCount, ThreadCount, ItemsPerThread, ItemsPerThread>();

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
        TStatsCollector collector = TBenchCases<TMPMCRingQueue<SizeBits>, TFastQueue<SizeBits>>::BasicProducingConsuming<TStatsCollector, ThreadCount, ThreadCount, ItemsPerThread, ItemsPerThread>();

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
        TStatsCollector collector = TBenchCases<TMPMCRingQueue<SizeBits>, TSlowQueue<SizeBits>>::BasicProducingConsuming<TStatsCollector, ThreadCount, ThreadCount, ItemsPerThread, ItemsPerThread>();

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
        TStatsCollector collector = TBenchCases<TMPMCRingQueue<SizeBits>, TVerySlowQueue<SizeBits>>::BasicProducingConsuming<TStatsCollector, ThreadCount, ThreadCount, ItemsPerThread, ItemsPerThread>();

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
        TStatsCollector collector = TBenchCases<TMPMCRingQueue<SizeBits>, TSingleQueue<SizeBits>>::BasicProducingConsuming<TStatsCollector, ThreadCount, 1, ItemsPerThread, ThreadCount * ItemsPerThread>();

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
        TStatsCollector collector = TBenchCases<TMPMCRingQueue<SizeBits>, TAdaptiveQueue<SizeBits>>::BasicProducingConsuming<TStatsCollector, ThreadCount, ThreadCount, ItemsPerThread, ItemsPerThread>();

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
