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
struct TMPMCRingQueueV3 {

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
        ui64 Generation = 0;
        ui64 Value = 0;
        bool IsEmpty;
        bool IsOvertaken;

        static constexpr ui64 MakeEmpty(ui64 generation) {
            return EmptyBit | generation;
        }

        static constexpr ui64 MakeOvertaken(ui64 generation) {
            return OvertakenBit | generation;
        }

        static constexpr TSlot Recognise(ui64 slotValue) {
            if (slotValue & EmptyBit) {
                return {.Generation = (EmptyBit ^ slotValue), .IsEmpty = true};
            }
            if (slotValue & OvertakenBit) {
                return {.Generation = (OvertakenBit ^ slotValue), .IsEmpty = true, .IsOvertaken = true};
            }
            return {.Value = slotValue, .IsEmpty = false};
        }
    };

    struct THeadTail {
        ui32 Head = 0;
        ui32 Tail = 0;

        static THeadTail Recognise(ui64 headTail) {
            return {
                .Head = static_cast<ui32>((headTail >> MaxSizeBits) & ((1ull << MaxSizeBits) - 1)),
                .Tail = static_cast<ui32>(headTail & ((1ull << MaxSizeBits) - 1)),
            };
        }

        ui64 Compact() const {
            return (Head << MaxSizeBits) + (Tail & ((1ull << MaxSizeBits) - 1));
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

    TMPMCRingQueueV3()
        : Buffer(new std::atomic<ui64>[MaxSize])
    {
        for (ui32 idx = 0; idx < MaxSize; ++idx) {
            Buffer[idx] = TSlot::MakeEmpty(0);
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
            TSlot slot;
            ui64 expected = TSlot::MakeEmpty(0);
            do {
                if (currentSlot.compare_exchange_strong(expected, val, std::memory_order_acq_rel)) {
                    OBSERVE(SuccessFastPush);
                    if (slot.IsOvertaken) {
                        AddOvertakenSlot(slotIdx);
                        OvertakenSlots.fetch_add(1, std::memory_order_acq_rel);
                    }
                    return true;
                }
                slot = TSlot::Recognise(expected);
            } while (slot.IsEmpty);

            if (!slot.IsEmpty) {
                ui64 currentHead = Head.load(std::memory_order_acquire);
                if (currentHead + MaxSize <= currentTail + std::min<ui64>(64, MaxSize - 1)) {
                    OBSERVE(FailedPush);
                    currentTail++;
                    Tail.compare_exchange_strong(currentTail, currentTail - 1, std::memory_order_acq_rel);
                    return false;
                }
            }

            SpinLockPause();
        }
    }

    std::optional<ui32> TryPopFromOvertakenSlots() {
        ui32 el = OvertakenQueue.Pop(ReadRevolvingCounter.fetch_add(1, std::memory_order_relaxed));
        if (el) {
            std::atomic<ui64> &currentSlot = Buffer[el - 1];
            ui64 expected = currentSlot.load(std::memory_order_acquire);
            TSlot slot = TSlot::Recognise(expected);
            ui64 generation = Head.load(std::memory_order_acquire) / MaxSize;
            while (generation >= slot.Generation) {
                if (!slot.IsEmpty) {
                    if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(0))) {
                        if (!slot.IsEmpty) {
                            return slot.Value;
                        }
                        break;
                    }
                } else {
                    return std::nullopt;
                }
                slot = TSlot::Recognise(expected);
            }
        }
        return std::nullopt;
    }

    void AddOvertakenSlot(ui64 slotIdx) {
        return OvertakenQueue.Push(slotIdx + 1, WriteRevolvingCounter.fetch_add(1, std::memory_order_relaxed));
    }

    std::optional<ui32> InvalidateSlot(std::atomic<ui64> &currentSlot, ui64 generation) {
        ui64 expected = currentSlot.load(std::memory_order_acquire);
        TSlot slot = TSlot::Recognise(expected);
        while (!slot.IsEmpty || slot.Generation <= generation) {
            if (currentSlot.compare_exchange_strong(expected, TSlot::MakeEmpty(generation + 1))) {
                if (!slot.IsEmpty) {
                    return slot.Value;
                }
                return std::nullopt;
            }
        }
        return std::nullopt;
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

        for (ui32 it = 0;; ++it) {
            OBSERVE_WITH_CONDITION(LongFastPop10It, it == 10);
            OBSERVE_WITH_CONDITION(LongFastPop100It, it == 100);
            OBSERVE_WITH_CONDITION(LongFastPop1000It, it == 1000);

            ui64 currentHead = Head.fetch_add(1, std::memory_order_relaxed);
            OBSERVE(AfterReserveSlotInFastPop);
            ui32 generation = currentHead / MaxSize;

            ui64 slotIdx = ConvertIdx(currentHead);
            std::atomic<ui64> &currentSlot = Buffer[slotIdx];

            ui64 expected = currentSlot.load(std::memory_order_relaxed);
            TSlot slot = TSlot::Recognise(expected);

            bool skipIteration = false;
            while (generation >= slot.Generation) {
                if (slot.IsEmpty) {
                    if (currentSlot.compare_exchange_weak(expected, TSlot::MakeOvertaken(0))) {
                        break;
                    }
                } else if (slot.IsOvertaken) {
                    skipIteration = true;
                    break;
                } else {
                    if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(0))) {
                        if (!slot.IsEmpty) {
                            OBSERVE(SuccessFastPop);
                            return slot.Value;
                        }
                        break;
                    }
                }
                slot = TSlot::Recognise(expected);
            }
            if (skipIteration) {
                OBSERVE(FailedFastPopAttempt);
                SpinLockPause();
                continue;
            }

            if (slot.Generation > generation) {
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