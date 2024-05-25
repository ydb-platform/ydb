#define MPMC_RING_QUEUE_COLLECT_STATISTICS

#include "mpmc_ring_queue.h"
#include "mpmc_ring_queue_ut_base.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

#include <queue>


using namespace NActors;
using namespace NActors::NTests;

namespace { // Tests

    template <ui32 SizeBits, template <ui32> typename TQueueAdaptor>
    struct TTestCases {
        static constexpr ui32 MaxSize = 1 << SizeBits;

        static auto GetHead(const TMPMCRingQueue<SizeBits> &realQueue) {
            if constexpr (std::is_same_v<TQueueAdaptor<SizeBits>, TSingleQueue<SizeBits>>) {
                return realQueue.LocalHead;
            } else {
                return realQueue.Head.load();
            }
        }

        static auto GetHeadGeneration(const TMPMCRingQueue<SizeBits> &realQueue) {
            if constexpr (std::is_same_v<TQueueAdaptor<SizeBits>, TSingleQueue<SizeBits>>) {
                return realQueue.LocalGeneration;
            } else {
                return realQueue.Head.load() / MaxSize;
            }
        }

        static auto GetTail(const TMPMCRingQueue<SizeBits> &realQueue) {
            return realQueue.Tail.load();
        }

        static auto GetTailGeneration(const TMPMCRingQueue<SizeBits> &realQueue) {
            return realQueue.Tail.load() / MaxSize;
        }

        static void PushesPopsWithShift() {
            TMPMCRingQueue<SizeBits> realQueue;
            TQueueAdaptor<SizeBits> adaptor(&realQueue);
            
            for (ui32 it = 0; it < MaxSize; ++it) {
                for (ui32 idx = 0; idx < MaxSize - 1; ++idx) {
                    TString debugString = TStringBuilder() << "iteration:" << it << " pos: " << idx << "/" << MaxSize;
                    ui64 emptyCurrentGeneration = (ui64(1) << 63) + GetTailGeneration(realQueue);
                    auto head = GetHead(realQueue);
                    auto tail = GetTail(realQueue);
                    auto realIdx = tail % MaxSize;
                    UNIT_ASSERT_VALUES_EQUAL_C(realQueue.Buffer[realIdx].load(), emptyCurrentGeneration, debugString);
                    UNIT_ASSERT_C(adaptor.TryPush(idx), debugString);
                    UNIT_ASSERT_VALUES_EQUAL_C(realQueue.Buffer[realIdx].load(), idx, debugString);
                    UNIT_ASSERT_VALUES_EQUAL_C(head, GetHead(realQueue), debugString);
                    UNIT_ASSERT_VALUES_EQUAL_C(tail + 1, GetTail(realQueue), debugString);
                }

                for (ui32 idx = 0; idx < MaxSize - 1; ++idx) {
                    TString debugString = TStringBuilder() << "iteration:" << it << " pos: " << idx << "/" << MaxSize;
                    ui64 emptyNextGeneration = (ui64(1) << 63) + GetHeadGeneration(realQueue) + 1;
                    auto head = GetHead(realQueue);
                    auto tail = GetTail(realQueue);
                    auto realIdx = head % MaxSize;
                    UNIT_ASSERT_VALUES_EQUAL_C(realQueue.Buffer[realIdx].load(), idx, debugString);
                    std::optional<ui32> value = adaptor.TryPop();
                    UNIT_ASSERT_C(value, debugString);
                    if constexpr (std::is_same_v<TQueueAdaptor<SizeBits>, TSingleQueue<SizeBits>>) {
                        UNIT_ASSERT_VALUES_EQUAL_C((head + 1) % MaxSize, GetHead(realQueue), debugString);
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL_C(head + 1, GetHead(realQueue), debugString);
                    }
                    UNIT_ASSERT_VALUES_EQUAL_C(tail, realQueue.Tail.load(), debugString);
                    UNIT_ASSERT_VALUES_EQUAL_C(realQueue.Buffer[realIdx].load(), emptyNextGeneration,debugString);
                    UNIT_ASSERT_VALUES_EQUAL_C(*value, idx, debugString);
                }
            }
        }

        static void PushesOverloadPops() {
            TMPMCRingQueue<SizeBits> realQueue;
            TQueueAdaptor<SizeBits> adaptor(&realQueue);
            
            for (ui32 it = 0; it < MaxSize; ++it) {
                for (ui32 idx = 0; idx < MaxSize; ++idx) {
                    TString debugString = TStringBuilder() << "iteration:" << it << " pos: " << idx << "/" << MaxSize;
                    ui64 emptyCurrentGeneration = (ui64(1) << 63) + (ui64)it;
                    auto head = GetHead(realQueue);
                    auto tail = GetTail(realQueue);
                    UNIT_ASSERT_VALUES_EQUAL_C(realQueue.Buffer[idx].load(), emptyCurrentGeneration, debugString);
                    UNIT_ASSERT_C(adaptor.TryPush(idx), debugString);
                    UNIT_ASSERT_VALUES_EQUAL_C(head, GetHead(realQueue), debugString);
                    UNIT_ASSERT_VALUES_EQUAL_C(tail + 1, GetTail(realQueue), debugString);
                }

                for (ui32 idx = 0; idx < MaxSize; ++idx) {
                    TString debugString = TStringBuilder() << "iteration:" << it << " pos: " << idx << "/" << MaxSize;
                    ui64 emptyNextGeneration = (ui64(1) << 63) + (ui64)it + 1;
                    auto head = GetHead(realQueue);
                    auto tail = GetTail(realQueue);
                    UNIT_ASSERT_VALUES_EQUAL_C(realQueue.Buffer[idx].load(), idx, debugString);
                    std::optional<ui32> value = adaptor.TryPop();
                    UNIT_ASSERT_C(value, debugString);
                    if constexpr (std::is_same_v<TQueueAdaptor<SizeBits>, TSingleQueue<SizeBits>>) {
                        UNIT_ASSERT_VALUES_EQUAL_C((head + 1) % MaxSize, GetHead(realQueue), debugString);
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL_C(head + 1, GetHead(realQueue), debugString);
                    }
                    UNIT_ASSERT_VALUES_EQUAL_C(tail, realQueue.Tail.load(), debugString);
                    UNIT_ASSERT_VALUES_EQUAL_C(realQueue.Buffer[idx].load(), emptyNextGeneration,debugString);
                    UNIT_ASSERT_VALUES_EQUAL_C(*value, idx, debugString);
                }
            }
        }

        static void CheckPushes() {
            if constexpr (MaxSize < 3) {
                return;
            }
            TMPMCRingQueue<SizeBits> realQueue;
            TQueueAdaptor<SizeBits> adaptor(&realQueue);

            ui64 emptyZeroGeneration = (ui64(1) << 63);

            UNIT_ASSERT(adaptor.TryPush(0));
            UNIT_ASSERT(adaptor.TryPush(1));
            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[0].load(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[1].load(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[2].load(), emptyZeroGeneration);

            for (ui32 idx = 2; idx < MaxSize; ++idx) {
                UNIT_ASSERT(adaptor.TryPush(idx));
            }

            for (ui32 idx = 0; idx < MaxSize; ++idx) {
                std::optional<ui32> value = adaptor.TryPop();
                UNIT_ASSERT(value);
                UNIT_ASSERT_VALUES_EQUAL(*value, idx);
            }

            ui64 emptyFirstGeneration = (ui64(1) << 63) + ui64(1);

            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[0].load(), emptyFirstGeneration);
            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[1].load(), emptyFirstGeneration);
            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[2].load(), emptyFirstGeneration);
            UNIT_ASSERT(adaptor.TryPush(0));
            UNIT_ASSERT(adaptor.TryPush(1));
            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[0].load(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[1].load(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[2].load(), emptyFirstGeneration);
        }

        static void CheckSlowPops() {
            TMPMCRingQueue<SizeBits> realQueue;
            TQueueAdaptor<SizeBits> adaptor(&realQueue);
            ui64 emptyZeroGeneration = (ui64(1) << 63);
            for (ui32 it = 0; it < MaxSize; ++it) {
                for (ui32 idx = 0; idx < MaxSize; ++idx) {
                    auto head = realQueue.Head.load();
                    auto tail = realQueue.Tail.load();
                    UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[idx].load(), emptyZeroGeneration);
                    std::optional<ui32> value = adaptor.TryPop();
                    UNIT_ASSERT_VALUES_EQUAL(head, realQueue.Head.load());
                    UNIT_ASSERT_VALUES_EQUAL(tail, realQueue.Tail.load());
                    UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[idx].load(), emptyZeroGeneration);
                    UNIT_ASSERT(!value);
                }
            }
        }

        static void CheckFastPops() {
            TMPMCRingQueue<SizeBits> realQueue;
            TQueueAdaptor<SizeBits> adaptor(&realQueue);
            for (ui32 it = 0; it < MaxSize; ++it) {
                for (ui32 idx = 0; idx < MaxSize; ++idx) {
                    ui64 emptyCurrentGeneration = (ui64(1) << 63) + (ui64)it;
                    ui64 emptyNextGeneration = (ui64(1) << 63) + (ui64)it + 1;
                    auto head = realQueue.Head.load();
                    auto tail = realQueue.Tail.load();
                    UNIT_ASSERT_VALUES_EQUAL_C(realQueue.Buffer[idx].load(), emptyCurrentGeneration, "iteration:" << it << " pos: " << idx << "/" << MaxSize);
                    std::optional<ui32> value = adaptor.TryPop();
                    UNIT_ASSERT_VALUES_EQUAL(tail + 1, realQueue.Tail.load());
                    UNIT_ASSERT_VALUES_EQUAL(head + 1, realQueue.Head.load());
                    UNIT_ASSERT_VALUES_EQUAL_C(realQueue.Buffer[idx].load(), emptyNextGeneration, "iteration:" << it << " pos: " << idx << "/" << MaxSize);
                    UNIT_ASSERT(!value);
                }
            }
        }
    };

    template<typename ...TQueues>
    void TestRandomUsage(ui32 iterationCount, ui32 MaxSize, TQueues ...args) {
        std::vector<IQueue*> queues {&args...};
        SetRandomSeed(727);
        std::queue<ui32> validationQueue;

        for (ui32 it = 0; it < iterationCount; ++it) {
            ui32 queueIdx = RandomNumber<ui32>(queues.size());
            IQueue &queue = *queues[queueIdx];
            bool isPush = RandomNumber<ui32>(2);
            TString debugString = TStringBuilder() << "it: " << it << " queue: " << queueIdx << " action: " << (isPush ? "push" : "pop") << " size: " << validationQueue.size() << '/' << MaxSize;
            if (isPush) {
                if (validationQueue.size() == MaxSize) {
                    UNIT_ASSERT_C(!queue.TryPush(it), debugString);
                } else {
                    UNIT_ASSERT_C(queue.TryPush(it), debugString);
                    validationQueue.push(it);
                }
            } else {
                if (validationQueue.empty()) {
                    UNIT_ASSERT_C(!queue.TryPop(), debugString);
                } else {
                    auto value = queue.TryPop();
                    UNIT_ASSERT_C(value, debugString);
                    UNIT_ASSERT_VALUES_EQUAL_C(*value, validationQueue.front(), debugString);
                    validationQueue.pop();
                }
            }
        }
    }


}

#define BASIC_QUEUE_TEST_CASES(SIZE_BITS, QUEUE)                 \
    Y_UNIT_TEST(PushesPopsWithShift_ ## QUEUE) {                 \
        TTestCases<SIZE_BITS, QUEUE>::PushesPopsWithShift();     \
    }                                                            \
                                                                 \
    Y_UNIT_TEST(PushesOverloadPops_ ## QUEUE) {                  \
        TTestCases<SIZE_BITS, QUEUE>::PushesOverloadPops();      \
    }                                                            \
                                                                 \
    Y_UNIT_TEST(CheckPushes_ ## QUEUE) {                         \
        TTestCases<SIZE_BITS, QUEUE>::CheckPushes();             \
    }                                                            \
// end BASIC_QUEUE_TEST_CASES

#define CHECK_SLOW_POPS(SIZE_BITS, QUEUE)                  \
    Y_UNIT_TEST(CheckSlowPops_ ## QUEUE) {                 \
        TTestCases<SIZE_BITS, QUEUE>::CheckSlowPops();     \
    }                                                      \
// end CHECK_SLOW_POPS

#define CHECK_FAST_POPS(SIZE_BITS, QUEUE)                  \
    Y_UNIT_TEST(CheckFastPops_ ## QUEUE) {                 \
        TTestCases<SIZE_BITS, QUEUE>::CheckFastPops();     \
    }                                                      \
// end CHECK_FAST_POPS

constexpr ui32 SizeBits = 3;

Y_UNIT_TEST_SUITE(MPMCRingQueueSingleThreadTests) {
    BASIC_QUEUE_TEST_CASES(SizeBits, TSingleQueue);
    BASIC_QUEUE_TEST_CASES(SizeBits, TVerySlowQueue);
    BASIC_QUEUE_TEST_CASES(SizeBits, TSlowQueue);
    BASIC_QUEUE_TEST_CASES(SizeBits, TFastQueue);
    BASIC_QUEUE_TEST_CASES(SizeBits, TVeryFastQueue);
    BASIC_QUEUE_TEST_CASES(SizeBits, TAdaptiveQueue);

    CHECK_SLOW_POPS(SizeBits, TSingleQueue);
    CHECK_SLOW_POPS(SizeBits, TVerySlowQueue);
    CHECK_SLOW_POPS(SizeBits, TSlowQueue);
    CHECK_FAST_POPS(SizeBits, TFastQueue);
    CHECK_FAST_POPS(SizeBits, TVeryFastQueue);

    Y_UNIT_TEST(RandomUsageFast) {
        return;
        TMPMCRingQueue<SizeBits> realQueue;
        TestRandomUsage(
            10'000,
            (ui64(1) << SizeBits),
            TVeryFastQueue<SizeBits>(&realQueue),
            TFastQueue<SizeBits>(&realQueue)
        );
    }

    Y_UNIT_TEST(RandomUsageSlow) {
        TMPMCRingQueue<SizeBits> realQueue;
        TestRandomUsage(
            10'000,
            (ui64(1) << SizeBits),
            TVerySlowQueue<SizeBits>(&realQueue),
            TSlowQueue<SizeBits>(&realQueue)
        );
    }

    Y_UNIT_TEST(RandomUsageAll) {
        return;
        TMPMCRingQueue<SizeBits> realQueue;
        TestRandomUsage(
            100'000,
            (ui64(1) << SizeBits),
            TVerySlowQueue<SizeBits>(&realQueue),
            TSlowQueue<SizeBits>(&realQueue),
            TFastQueue<SizeBits>(&realQueue),
            TVeryFastQueue<SizeBits>(&realQueue)
        );
    }
}
