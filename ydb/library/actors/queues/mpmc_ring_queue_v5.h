#pragma once
#include "defs.h"

#include "observer/methods.h"

#include <ydb/library/actors/util/unordered_cache.h>
#include <library/cpp/threading/chunk_queue/queue.h>
#include <util/system/mutex.h>

#include <atomic>
#include <optional>

namespace NActors {

template <ui32 MaxSizeBits, typename TObserver=void>
struct TMPMCRingQueueV5 {

#define HAS_OBSERVE_METHOD(ENTRY_POINT) \
    HasStaticMethodObserve ## ENTRY_POINT <TObserver>
// HAS_OBSERVE_METHOD

#define OBSERVE_WITH_CONDITION(ENTRY_POINT, CONDITION) \
    if constexpr (HAS_OBSERVE_METHOD(ENTRY_POINT)) {               \
        if ((CONDITION)) {                             \
            TObserver::Observe ## ENTRY_POINT ();              \
        }                                              \
    } do {} while (false)                              \
// OBSERVE_WITH_CONDITION

#define OBSERVE(ENTRY_POINT) \
    OBSERVE_WITH_CONDITION(ENTRY_POINT, true)
// OBSERVE

    static constexpr ui32 MaxSize = 1 << MaxSizeBits;

    struct alignas(ui64) TSlot {
        static constexpr ui64 EmptyBit = 1ull << 63;
        static constexpr ui64 OvertakenBit = 1ull << 62;
        ui64 Value = 0;
        bool IsEmpty = false;
        bool IsOvertaken = false;

        static constexpr ui64 MakeEmpty() {
            return EmptyBit;
        }

        static constexpr ui64 MakeOvertaken() {
            return OvertakenBit;
        }

        static constexpr TSlot Recognise(ui64 slotValue) {
            if (slotValue == EmptyBit) {
                return {.IsEmpty = true};
            }
            if (slotValue == OvertakenBit) {
                return {.IsEmpty = true, .IsOvertaken = true};
            }
            return {.Value = slotValue};
        }
    };

    enum class EQueueMode {
        Default,
        Overtaken,
        FullQueue,
    };

    NThreading::TPadded<std::atomic<EQueueMode>> QueueMode{EQueueMode::Default};
    NThreading::TPadded<std::atomic<ui64>> Tail{0};
    NThreading::TPadded<std::atomic<ui64>> Head{0};
    NThreading::TPadded<TArrayHolder<std::atomic<ui64>>> Buffer;
    NThreading::TPadded<TUnorderedCache<ui32, 512, 4>> OvertakenQueue;
    NThreading::TPadded<std::atomic<ui64>> ReadRevolvingCounter{0};
    NThreading::TPadded<std::atomic<ui64>> WriteRevolvingCounter{0};
    NThreading::TPadded<std::atomic<ui64>> OvertakenSlots{0};

    static constexpr ui32 ConvertIdx(ui32 idx) {
        idx = idx % MaxSize;
        if constexpr (MaxSize < 0x100) {
            return idx;
        }
        // 0, 16, 32, .., 240,
        // 1, 17, 33, .., 241,
        // ...
        // 15, 31, 63, ..., 255,
        return (idx & ~0xff) | ((idx & 0xf) << 4) | ((idx >> 4) & 0xf);
    }

    static constexpr ui32 ConvertOvertakenIdx(ui32 idx) {
        idx = idx % 4096;
        if constexpr (MaxSize < 0x100) {
            return idx;
        }
        // 0, 16, 32, .., 240,
        // 1, 17, 33, .., 241,
        // ...
        // 15, 31, 63, ..., 255,
        return (idx & ~0xff) | ((idx & 0xf) << 4) | ((idx >> 4) & 0xf);
    }

    TMPMCRingQueueV5()
        : Buffer(new std::atomic<ui64>[MaxSize])
    {
        for (ui32 idx = 0; idx < MaxSize; ++idx) {
            Buffer[idx] = TSlot::MakeEmpty();
        }
    }

    ~TMPMCRingQueueV5() {
        for (ui32 idx = 0; idx < OvertakenSlots.load(std::memory_order_acquire); ++idx) {
            OvertakenQueue.Pop(idx);
        }
    }

    bool TryPush(ui32 val) {
        for (ui32 it = 0;; ++it) {
            EQueueMode queueMode = QueueMode.load(std::memory_order_relaxed);

            if (queueMode == EQueueMode::FullQueue) {
                ui64 currentTail = Tail.load(std::memory_order_acquire);
                ui64 currentHead = Head.load(std::memory_order_acquire);
                if (currentHead + MaxSize <= currentTail + std::min<ui64>(64, MaxSize - 1)) {
                    OBSERVE(FailedPush);
                    return false;
                }
                if (currentHead + MaxSize/4*3 > currentTail) {
                    QueueMode.store(EQueueMode::Default, std::memory_order_relaxed);
                }
            }

            OBSERVE_WITH_CONDITION(LongPush10It, it == 10);
            OBSERVE_WITH_CONDITION(LongPush100It, it == 100);
            OBSERVE_WITH_CONDITION(LongPush1000It, it == 1000);
            ui64 currentTail = Tail.fetch_add(1, std::memory_order_relaxed);
            OBSERVE(AfterReserveSlotInFastPush);

            ui64 slotIdx = ConvertIdx(currentTail);

            std::atomic<ui64> &currentSlot = Buffer[slotIdx];
            ui64 expected = TSlot::MakeEmpty();
            TSlot slot = TSlot::Recognise(expected);
            while (slot.IsEmpty) {
                if (!slot.IsOvertaken) {
                    if (currentSlot.compare_exchange_strong(expected, val, std::memory_order_acq_rel)) {
                        OBSERVE(SuccessFastPush);
                        return true;
                    }
                } else {
                    if (currentSlot.compare_exchange_strong(expected, TSlot::MakeEmpty(), std::memory_order_acq_rel)) {
                        OBSERVE(SuccessFastPush);
                        if (slot.IsOvertaken) {
                            AddOvertakenSlot(val);
                            if (queueMode != EQueueMode::Overtaken) {
                                QueueMode.store(EQueueMode::Overtaken, std::memory_order_relaxed);
                            }
                        }
                        return true;
                    }
                }
                slot = TSlot::Recognise(expected);
            }

            if (!slot.IsEmpty) {
                ui64 currentHead = Head.load(std::memory_order_acquire);
                if (currentHead + MaxSize <= currentTail + std::min<ui64>(64, MaxSize - 1)) {
                    OBSERVE(FailedPush);
                    if (queueMode != EQueueMode::FullQueue) {
                        QueueMode.store(EQueueMode::FullQueue, std::memory_order_relaxed);
                    }
                    return false;
                }
            }

            SpinLockPause();
        }
    }

    struct TOverreadedSlot {};
    struct TFailedToGetSlot {};

    ui32 TryPopFromOvertakenSlots() {
        return OvertakenQueue.Pop(ReadRevolvingCounter.fetch_add(1, std::memory_order_relaxed));
    }

    void AddOvertakenSlot(ui64 val) {
        OvertakenQueue.Push(val, WriteRevolvingCounter.fetch_add(1, std::memory_order_relaxed));
        OvertakenSlots.fetch_add(1, std::memory_order_acq_rel);
    }

    std::optional<ui32> TryPop() {
        ui64 overtakenSlots = OvertakenSlots.load(std::memory_order_acquire);
        if (overtakenSlots) {
            bool success = false;
            while (overtakenSlots) {
                if (OvertakenSlots.compare_exchange_strong(overtakenSlots, overtakenSlots - 1, std::memory_order_acq_rel)) {
                    success = true;
                    break;
                }
                SpinLockPause();
            }
            if (success) {
                for (;;) {
                    if (auto el = TryPopFromOvertakenSlots()) {
                        OBSERVE(SuccessOvertakenPop);
                        return el;
                    }
                    SpinLockPause();
                }
            }
        }

        EQueueMode queueMode = QueueMode.load(std::memory_order_relaxed);
        if (queueMode == EQueueMode::Overtaken) {
            ui64 currentHead = Head.load(std::memory_order_acquire);
            ui64 currentTail = Tail.load(std::memory_order_acquire);
            if (currentHead > currentTail) {
                OBSERVE(FailedFastPop);
                return std::nullopt;
            }
            if (currentHead + 10 < currentTail) {
                QueueMode.store(EQueueMode::Default, std::memory_order_relaxed);
            }
        }

        for (ui32 it = 0;; ++it) {
            OBSERVE_WITH_CONDITION(LongFastPop10It, it == 10);
            OBSERVE_WITH_CONDITION(LongFastPop100It, it == 100);
            OBSERVE_WITH_CONDITION(LongFastPop1000It, it == 1000);

            ui64 currentHead = Head.fetch_add(1, std::memory_order_relaxed);
            OBSERVE(AfterReserveSlotInFastPop);

            ui64 slotIdx = ConvertIdx(currentHead);
            std::atomic<ui64> &currentSlot = Buffer[slotIdx];

            ui64 expected = currentSlot.load(std::memory_order_relaxed);
            TSlot slot = TSlot::Recognise(expected);

            while (!slot.IsEmpty || !slot.IsOvertaken) {
                if (slot.IsEmpty) {
                    if (currentSlot.compare_exchange_weak(expected, TSlot::MakeOvertaken())) {
                        break;
                    }
                } else {
                    if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty())) {
                        OBSERVE(SuccessFastPop);
                        return slot.Value;
                    }
                }
                slot = TSlot::Recognise(expected);
            }
            if (slot.IsOvertaken) {
                OBSERVE(FailedFastPopAttempt);
                SpinLockPause();
                continue;
            }

            ui64 currentTail = Tail.load(std::memory_order_acquire);
            if (currentTail <= currentHead) {
                OBSERVE(FailedFastPop);
                return std::nullopt;
            }

            OBSERVE(FailedFastPopAttempt);
            SpinLockPause();
        }
    }

#undef OBSERVE_WITH_CONDITION
#undef OBSERVE
#undef HAS_OBSERVE_METHOD
};

}  // NActors