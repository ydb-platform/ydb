#define MPMC_RING_QUEUE_COLLECT_STATISTICS

#include "mpmc_ring_queue.h"
#include "bench/queue.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

#include <queue>


using namespace NActors;
using namespace NActors::NQueueBench;

namespace { // Tests

    template <ui32 SizeBits>
    struct TTestCases {
        static constexpr ui32 MaxSize = 1 << SizeBits;

        static auto GetHead(const TMPMCRingQueueV2WithStats<SizeBits> &realQueue) {
            return realQueue.Head.load();
        }

        static auto GetHeadGeneration(const TMPMCRingQueueV2WithStats<SizeBits> &realQueue) {
            return realQueue.Head.load() / MaxSize;
        }

        static auto GetTail(const TMPMCRingQueueV2WithStats<SizeBits> &realQueue) {
            return realQueue.Tail.load();
        }

        static auto GetTailGeneration(const TMPMCRingQueueV2WithStats<SizeBits> &realQueue) {
            return realQueue.Tail.load() / MaxSize;
        }

        static void PushesPopsWithShift() {
            TMPMCRingQueueV2WithStats<SizeBits> realQueue;

            for (ui32 it = 0; it < MaxSize; ++it) {
                for (ui32 idx = 0; idx < MaxSize - 1; ++idx) {
                    TString debugString = TStringBuilder() << "iteration:" << it << " pos: " << idx << "/" << MaxSize;
                    ui64 emptyCurrentGeneration = (ui64(1) << 63) + GetTailGeneration(realQueue);
                    auto head = GetHead(realQueue);
                    auto tail = GetTail(realQueue);
                    auto realIdx = tail % MaxSize;
                    UNIT_ASSERT_VALUES_EQUAL_C(realQueue.Buffer[realIdx].load(), emptyCurrentGeneration, debugString);
                    UNIT_ASSERT_C(realQueue.TryPush(idx), debugString);
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
                    std::optional<ui32> value = realQueue.TryPop();
                    UNIT_ASSERT_C(value, debugString);
                    UNIT_ASSERT_VALUES_EQUAL_C(head + 1, GetHead(realQueue), debugString);
                    UNIT_ASSERT_VALUES_EQUAL_C(tail, realQueue.Tail.load(), debugString);
                    UNIT_ASSERT_VALUES_EQUAL_C(realQueue.Buffer[realIdx].load(), emptyNextGeneration,debugString);
                    UNIT_ASSERT_VALUES_EQUAL_C(*value, idx, debugString);
                }
            }
        }

        static void PushesOverloadPops() {
            TMPMCRingQueueV2WithStats<SizeBits> realQueue;

            for (ui32 it = 0; it < MaxSize; ++it) {
                for (ui32 idx = 0; idx < MaxSize; ++idx) {
                    TString debugString = TStringBuilder() << "iteration:" << it << " pos: " << idx << "/" << MaxSize;
                    ui64 emptyCurrentGeneration = (ui64(1) << 63) + (ui64)it;
                    auto head = GetHead(realQueue);
                    auto tail = GetTail(realQueue);
                    UNIT_ASSERT_VALUES_EQUAL_C(realQueue.Buffer[idx].load(), emptyCurrentGeneration, debugString);
                    UNIT_ASSERT_C(realQueue.TryPush(idx), debugString);
                    UNIT_ASSERT_VALUES_EQUAL_C(head, GetHead(realQueue), debugString);
                    UNIT_ASSERT_VALUES_EQUAL_C(tail + 1, GetTail(realQueue), debugString);
                }

                for (ui32 idx = 0; idx < MaxSize; ++idx) {
                    TString debugString = TStringBuilder() << "iteration:" << it << " pos: " << idx << "/" << MaxSize;
                    ui64 emptyNextGeneration = (ui64(1) << 63) + (ui64)it + 1;
                    auto head = GetHead(realQueue);
                    auto tail = GetTail(realQueue);
                    UNIT_ASSERT_VALUES_EQUAL_C(realQueue.Buffer[idx].load(), idx, debugString);
                    std::optional<ui32> value = realQueue.TryPop();
                    UNIT_ASSERT_C(value, debugString);
                    UNIT_ASSERT_VALUES_EQUAL_C(head + 1, GetHead(realQueue), debugString);
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
            TMPMCRingQueueV2WithStats<SizeBits> realQueue;

            ui64 emptyZeroGeneration = (ui64(1) << 63);

            UNIT_ASSERT(realQueue.TryPush(0));
            UNIT_ASSERT(realQueue.TryPush(1));
            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[0].load(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[1].load(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[2].load(), emptyZeroGeneration);

            for (ui32 idx = 2; idx < MaxSize; ++idx) {
                UNIT_ASSERT(realQueue.TryPush(idx));
            }

            for (ui32 idx = 0; idx < MaxSize; ++idx) {
                std::optional<ui32> value = realQueue.TryPop();
                UNIT_ASSERT(value);
                UNIT_ASSERT_VALUES_EQUAL(*value, idx);
            }

            ui64 emptyFirstGeneration = (ui64(1) << 63) + ui64(1);

            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[0].load(), emptyFirstGeneration);
            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[1].load(), emptyFirstGeneration);
            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[2].load(), emptyFirstGeneration);
            UNIT_ASSERT(realQueue.TryPush(0));
            UNIT_ASSERT(realQueue.TryPush(1));
            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[0].load(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[1].load(), 1u);
            UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[2].load(), emptyFirstGeneration);
        }

        static void CheckSlowPops() {
            TMPMCRingQueueV2WithStats<SizeBits> realQueue;
            ui64 emptyZeroGeneration = (ui64(1) << 63);
            for (ui32 it = 0; it < MaxSize; ++it) {
                for (ui32 idx = 0; idx < MaxSize; ++idx) {
                    auto head = realQueue.Head.load();
                    auto tail = realQueue.Tail.load();
                    UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[idx].load(), emptyZeroGeneration);
                    std::optional<ui32> value = realQueue.TryPop();
                    UNIT_ASSERT_VALUES_EQUAL(head, realQueue.Head.load());
                    UNIT_ASSERT_VALUES_EQUAL(tail, realQueue.Tail.load());
                    UNIT_ASSERT_VALUES_EQUAL(realQueue.Buffer[idx].load(), emptyZeroGeneration);
                    UNIT_ASSERT(!value);
                }
            }
        }

        static void CheckFastPops() {
            TMPMCRingQueueV2WithStats<SizeBits> realQueue;
            for (ui32 it = 0; it < MaxSize; ++it) {
                for (ui32 idx = 0; idx < MaxSize; ++idx) {
                    ui64 emptyCurrentGeneration = (ui64(1) << 63) + (ui64)it;
                    ui64 emptyNextGeneration = (ui64(1) << 63) + (ui64)it + 1;
                    auto head = realQueue.Head.load();
                    auto tail = realQueue.Tail.load();
                    UNIT_ASSERT_VALUES_EQUAL_C(realQueue.Buffer[idx].load(), emptyCurrentGeneration, "iteration:" << it << " pos: " << idx << "/" << MaxSize);
                    std::optional<ui32> value = realQueue.TryPop();
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

constexpr ui32 SizeBits = 3;

Y_UNIT_TEST_SUITE(MPMCRingQueueV2SingleThreadTests) {
    Y_UNIT_TEST(PushesPopsWithShift) {
        TTestCases<SizeBits>::PushesPopsWithShift();
    }

    Y_UNIT_TEST(PushesOverloadPops) {
        TTestCases<SizeBits>::PushesOverloadPops();
    }

    Y_UNIT_TEST(CheckPushes) {
        TTestCases<SizeBits>::CheckPushes();
    }

    Y_UNIT_TEST(RandomUsage) {
        TMPMCRingQueueV2WithStats<SizeBits> realQueue;
        TestRandomUsage(1024, (ui64(1) << SizeBits), TIdAdaptor<TMPMCRingQueueV2WithStats<SizeBits>>(&realQueue));
    }
}
