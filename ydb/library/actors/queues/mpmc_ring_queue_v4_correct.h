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
struct TMPMCRingQueueV4Correct {

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
        static constexpr ui64 OvertakenMask = 0xffffull << 32;
        ui32 Value = 0;
        i16 Overtakens = 0;
        bool IsEmpty = false;

        static constexpr ui64 MakeEmpty(i16 overtakens) {
            return EmptyBit | (static_cast<ui64>(static_cast<ui16>(overtakens)) << 32);
        }

        static constexpr ui64 MakeValue(ui32 value, i16 overtakens) {
            return static_cast<ui64>(value) | (static_cast<ui64>(static_cast<ui16>(overtakens)) << 32);
        }

        static constexpr TSlot Recognise(ui64 slotValue) {
            return {
                .Value = static_cast<ui32>(slotValue),
                .Overtakens = static_cast<i16>(static_cast<ui16>(slotValue >> 32)),
                .IsEmpty = (slotValue & EmptyBit) != 0,
            };
        }
    };

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

    TMPMCRingQueueV4Correct(ui64)
        : Buffer(new std::atomic<ui64>[MaxSize])
    {
        for (ui32 idx = 0; idx < MaxSize; ++idx) {
            Buffer[idx] = TSlot::MakeEmpty(0);
        }
    }

    ~TMPMCRingQueueV4Correct() {
        for (ui32 idx = 0; idx < OvertakenSlots.load(std::memory_order_acquire); ++idx) {
            OvertakenQueue.Pop(idx);
        }
    }

    bool TryPush(ui32 val) {
        for (ui32 it = 0;; ++it) {
            OBSERVE_WITH_CONDITION(LongPush10It, it == 10);
            OBSERVE_WITH_CONDITION(LongPush100It, it == 100);
            OBSERVE_WITH_CONDITION(LongPush1000It, it == 1000);
            ui64 currentTail = Tail.fetch_add(1, std::memory_order_relaxed);
            OBSERVE(AfterReserveSlotInFastPush);

            ui64 slotIdx = ConvertIdx(currentTail);

            std::atomic<ui64> &currentSlot = Buffer[slotIdx];
            ui64 expected = TSlot::MakeEmpty(0);
            TSlot slot = TSlot::Recognise(expected);

            while (true) {
                if (slot.IsEmpty) {
                    if (slot.Overtakens <= 0) {
                        ui64 newSlotValue = TSlot::MakeValue(val, slot.Overtakens);
                        if (currentSlot.compare_exchange_strong(expected, newSlotValue, std::memory_order_acq_rel)) {
                            OBSERVE(SuccessFastPush);
                            return true;
                        }
                    } else {
                        ui64 newSlotValue = TSlot::MakeEmpty(slot.Overtakens - 1);
                        if (currentSlot.compare_exchange_strong(expected, newSlotValue, std::memory_order_acq_rel)) {
                            OBSERVE(SuccessFastPush);
                            AddOvertakenSlot(val);
                            return true;
                        }
                    }
                } else {
                    ui64 newSlotValue = TSlot::MakeValue(slot.Value, slot.Overtakens - 1);
                    if (currentSlot.compare_exchange_strong(expected, newSlotValue, std::memory_order_acq_rel)) {
                        break;
                    }
                }
                slot = TSlot::Recognise(expected);
            }

            // Now we need to decide, continue to push or not
            ui64 currentHead = Head.load(std::memory_order_acquire);
            if (currentHead + MaxSize <= currentTail + std::min<ui64>(64, MaxSize - 1)) {
                OBSERVE(FailedPush);
                return false;
            }

            SpinLockPause();
        }
    }

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
            ui64 currentTail = Tail.load(std::memory_order_acquire);
            ui64 currentHead = Head.load(std::memory_order_acquire);
            if (currentHead >= currentTail) {
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

            while (true) {
                TSlot slot = TSlot::Recognise(expected);
                if (slot.IsEmpty) {
                    ui64 newSlotValue = TSlot::MakeEmpty(slot.Overtakens + 1);
                    if (currentSlot.compare_exchange_strong(expected, newSlotValue, std::memory_order_acq_rel)) {
                        break;
                    }
                } else {
                    ui64 newSlotValue = TSlot::MakeEmpty(slot.Overtakens);
                    if (currentSlot.compare_exchange_strong(expected, newSlotValue, std::memory_order_acq_rel)) {
                        OBSERVE(SuccessFastPop);
                        return slot.Value;
                    }
                }
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