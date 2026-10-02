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
struct TMPMCRingQueueV9 {


    struct TUI32BufferV9 {
        alignas(64) std::atomic<ui32> Count{0}; // 64 bytes
        alignas(64) std::atomic<ui32> Buffer[256];

        TUI32BufferV9() {
            for (ui32 idx = 0; idx < 256; ++idx) {
                Buffer[idx] = Max<ui32>();
            }
        }

        static constexpr ui32 ConvertIdx(ui32 idx) {
            // 0, 16, 32, .., 240,
            // 1, 17, 33, .., 241,
            // ...
            // 15, 31, 63, ..., 255,
            return ((idx & 0xf) << 4) | ((idx >> 4) & 0xf);
        }

        bool Push(ui32 val) {
            ui32 count = Count.load(std::memory_order_acquire);
            if (count == 255) {
                return false;
            }
            for (ui32 idx = 0; idx < 256; ++idx) {
                ui32 realIdx = ConvertIdx(idx);
                std::atomic<ui32> &currentSlot = Buffer[realIdx];
                ui32 expected = currentSlot.load(std::memory_order_relaxed);
                if (expected == Max<ui32>()) {
                    if (currentSlot.compare_exchange_strong(expected, val, std::memory_order_acq_rel)) {
                        Count.fetch_add(1, std::memory_order_release);
                        return true;
                    }
                }
            }
            return false;
        }

        std::optional<ui32> Pop() {
            ui32 count = Count.load(std::memory_order_acquire);
            if (count == 0) {
                return std::nullopt;
            }
            for (ui32 idx = 0; idx < 256; ++idx) {
                ui32 realIdx = ConvertIdx(idx);
                std::atomic<ui32> &currentSlot = Buffer[realIdx];
                ui32 expected = currentSlot.load(std::memory_order_relaxed);
                if (expected != Max<ui32>()) {
                    if (currentSlot.compare_exchange_strong(expected, 0, std::memory_order_acq_rel)) {
                        Count.fetch_sub(1, std::memory_order_release);
                        return expected;
                    }
                }
            }
            return std::nullopt;
        }

    };

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
        ui64 Value = 0;
        bool IsEmpty = false;

        static constexpr ui64 MakeEmpty() {
            return EmptyBit;
        }

        static constexpr TSlot Recognise(ui64 slotValue) {
            if (slotValue == EmptyBit) {
                return {.IsEmpty = true};
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
    //NThreading::TPadded<TUnorderedCache<ui32, 512, 4>> OvertakenQueue;
    //NThreading::TPadded<std::atomic<ui64>> ReadRevolvingCounter{0};
    //NThreading::TPadded<std::atomic<ui64>> WriteRevolvingCounter{0};
    //NThreading::TPadded<std::atomic<ui64>> OvertakenSlots{0};
    NThreading::TPadded<TUI32BufferV9> OvertakenQueue;
    ui32 ReadersCount = 0;

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

    TMPMCRingQueueV9(ui32 readersCount)
        : Buffer(new std::atomic<ui64>[MaxSize])
        , ReadersCount(readersCount / 2)
    {
        for (ui32 idx = 0; idx < MaxSize; ++idx) {
            Buffer[idx] = TSlot::MakeEmpty();
        }
    }

    ~TMPMCRingQueueV9() {
    }

    bool TryPush(ui32 val) {
        for (ui32 it = 0;; ++it) {
            EQueueMode queueMode = QueueMode.load(std::memory_order_relaxed);

            if (queueMode == EQueueMode::FullQueue) {
                ui64 currentTail = Tail.load(std::memory_order_acquire);
                ui64 slotIdx = ConvertIdx(currentTail);
                std::atomic<ui64> &currentSlot = Buffer[slotIdx];
                ui64 expected = currentSlot.load(std::memory_order_relaxed);
                TSlot slot = TSlot::Recognise(expected);
                if (!slot.IsEmpty) {
                    return false;
                }

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
                if (currentSlot.compare_exchange_strong(expected, val, std::memory_order_acq_rel)) {
                    OBSERVE(SuccessFastPush);
                    return true;
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

    std::optional<ui32> TryPopFromOvertakenSlots() {
        ui32 count = OvertakenQueue.Count.load(std::memory_order_acquire);
        if (count == 0) {
            return std::nullopt;
        }
        ui32 seen = 0;
        for (ui32 idx = 0; idx < 256 && seen < count; ++idx) {
            ui32 realIdx = TUI32BufferV8::ConvertIdx(idx);
            std::atomic<ui32> &overtakenSlot = OvertakenQueue.Buffer[realIdx];
            ui32 slotIdx = overtakenSlot.load(std::memory_order_relaxed);
            if (slotIdx != Max<ui32>()) {
                ++seen;
                std::atomic<ui64> &currentSlot = Buffer[slotIdx];
                ui64 expected = currentSlot.load(std::memory_order_relaxed);
                TSlot slot = TSlot::Recognise(expected);
                if (slot.IsEmpty) {
                    continue;
                }
                while (!slot.IsEmpty) {
                    if (currentSlot.compare_exchange_strong(expected, TSlot::MakeEmpty(), std::memory_order_acq_rel)) {
                        ui32 expectedSlotIdx = slotIdx;
                        while (expectedSlotIdx == slotIdx) {
                            if (overtakenSlot.compare_exchange_strong(slotIdx, Max<ui32>(), std::memory_order_acq_rel)) {
                                break;
                            }
                        }
                        OvertakenQueue.Count.fetch_sub(1, std::memory_order_release);
                        return slot.Value;
                    }
                    slot = TSlot::Recognise(expected);
                    SpinLockPause();
                }
            }
        }
        return std::nullopt;
    }

    void AddOvertakenSlot(ui64 val) {
        OvertakenQueue.Push(val);
        QueueMode.store(EQueueMode::Overtaken, std::memory_order_relaxed);
    }

    std::optional<ui32> TryPop() {
        EQueueMode queueMode = QueueMode.load(std::memory_order_relaxed);
        if (queueMode == EQueueMode::Overtaken) {
            auto el = TryPopFromOvertakenSlots();
            if (el) {
                OBSERVE(SuccessOvertakenPop);
                return el;
            }
            SpinLockPause();
        }

        if (queueMode == EQueueMode::Overtaken) {
            ui64 currentHead = Head.load(std::memory_order_acquire);
            ui64 slotIdx = ConvertIdx(currentHead);
            std::atomic<ui64> &currentSlot = Buffer[slotIdx];
            ui64 expected = currentSlot.load(std::memory_order_relaxed);
            TSlot slot = TSlot::Recognise(expected);
            if (slot.IsEmpty) {
                OBSERVE(FailedFastPop);
                return std::nullopt;
            }

            ui64 currentTail = Tail.load(std::memory_order_acquire);
            currentHead = Head.load(std::memory_order_acquire);
            if (currentHead + ReadersCount <= currentTail) {
                QueueMode.store(EQueueMode::Default, std::memory_order_relaxed);
            } else if (currentHead >= currentTail) {
                return std::nullopt;
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

            if (slot.IsEmpty) {
                AddOvertakenSlot(slotIdx);
                break;
            }

            while (!slot.IsEmpty) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty())) {
                    OBSERVE(SuccessFastPop);
                    return slot.Value;
                }
                slot = TSlot::Recognise(expected);
            }

            ui64 currentTail = Tail.load(std::memory_order_acquire);
            if (currentTail <= currentHead) {
                OBSERVE(FailedFastPop);
                return std::nullopt;
            }

            OBSERVE(FailedFastPopAttempt);
            SpinLockPause();
        }

        return std::nullopt;
    }

#undef OBSERVE_WITH_CONDITION
#undef OBSERVE
#undef HAS_OBSERVE_METHOD
};

}  // NActors