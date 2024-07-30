#pragma once
#include "defs.h"
#include "mpmc_bitmap_buffer.h"

#include "observer/methods.h"

#include <library/cpp/threading/chunk_queue/queue.h>
#include <util/system/mutex.h>

#include <atomic>
#include <optional>

namespace NActors {

template <ui32 MaxSizeBits, typename TObserver=void>
struct TMPMCRingQueueV2 {

#define HAS_OBSERVE_METHOD(ENTRY_POINT) \
    HasStaticMethodObserve ## ENTRY_POINT <TObserver>
// HAS_OBSERVE_METHOD

#define OBSERVE_WITH_CONDITION(ENTRY_POINT, CONDITION) \
    if constexpr (HAS_OBSERVE_METHOD(ENTRY_POINT)) {   \
        if ((CONDITION)) {                             \
            TObserver::Observe ## ENTRY_POINT ();      \
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
                return {.Generation = (EmptyBit ^ slotValue), .IsEmpty = true, .IsOvertaken = false};
            }
            if (slotValue & OvertakenBit) {
                return {.Generation = (OvertakenBit ^ slotValue), .IsEmpty = true, .IsOvertaken = true};
            }
            return {.Value = slotValue, .IsEmpty = false, .IsOvertaken = false};
        }
    };

    static constexpr ui64 PushStopBit = 1ull << 63;
    static constexpr ui64 HasOvertakenSlotsBit = 1ull << 63;
    NThreading::TPadded<std::atomic<ui64>> Tail{0};
    NThreading::TPadded<std::atomic<ui64>> Head{0};
    NThreading::TPadded<TArrayHolder<std::atomic<ui64>>> Buffer;
    static constexpr ui64 OvertakenBufferSizeBits = 6;
    NThreading::TPadded<TMPMCBitMapBuffer> OvertakenBuffer;
    ui64 LocalGeneration = 0;

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

    TMPMCRingQueueV2()
        : Buffer(new std::atomic<ui64>[MaxSize])
        , OvertakenBuffer(OvertakenBufferSizeBits)
    {
        for (ui32 idx = 0; idx < MaxSize; ++idx) {
            Buffer[idx] = TSlot::MakeEmpty(0);
        }
    }

    bool CheckPushOvertaken(ui64 tail) {
        ui64 currentHead = Head.load(std::memory_order_acquire) & ~HasOvertakenSlotsBit;
        return currentHead + MaxSize <= tail + std::min<ui64>(4096, MaxSize - 1);
    }

    bool TryPush(ui32 val) {
        // Cerr << "TryPush\n";
        for (ui32 it = 0;; ++it) {
            OBSERVE_WITH_CONDITION(LongPush10It, it == 10);
            OBSERVE_WITH_CONDITION(LongPush100It, it == 100);
            OBSERVE_WITH_CONDITION(LongPush1000It, it == 1000);
            // Cerr << "it: " << it << Endl;
            ui64 prevTail = Tail.load(std::memory_order_acquire);
            if (prevTail & HasOvertakenSlotsBit) {
                if (CheckPushOvertaken(prevTail ^ HasOvertakenSlotsBit)) {
                    OBSERVE(FailedPush);
                    return false;
                }
                Tail.compare_exchange_strong(prevTail, prevTail ^ HasOvertakenSlotsBit, std::memory_order_acq_rel);
            }
            ui64 currentTail = Tail.fetch_add(1, std::memory_order_relaxed) & ~HasOvertakenSlotsBit;
            ui64 generation = currentTail / MaxSize;
            OBSERVE(AfterReserveSlotInFastPush);

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentTail)];
            TSlot slot;
            ui64 expected = TSlot::MakeEmpty(generation);
            do {
                if (currentSlot.compare_exchange_strong(expected, val, std::memory_order_acq_rel)) {
                    OBSERVE(SuccessFastPush);
                    return true;
                }
                slot = TSlot::Recognise(expected);
            } while (slot.IsEmpty && slot.Generation <= generation);
            // Cerr << "slot.IsEmpty: " << (slot.IsEmpty ? "yes" : "no") << " slot.IsOvertaken: " << (slot.IsOvertaken ? "yes" : "no") << " slot.Generation: " << slot.Generation << " generation: " << generation << Endl;

            if (!slot.IsEmpty) {
                if (CheckPushOvertaken(currentTail)) {
                    OBSERVE(FailedPush);
                    Tail.compare_exchange_strong(currentTail, prevTail | HasOvertakenSlotsBit, std::memory_order_acq_rel);
                    return false;
                }
            }

            SpinLockPause();
        }
    }

    std::optional<ui32> TryPopFromOvertakenSlots(ui64 realHead) {
        // Cerr << "TryPopFromOvertakenSlots\n";
        std::optional<ui32> result;
        ui64 generation = realHead / MaxSize;
        // Cerr << "realHead: " << realHead << Endl;
        // Cerr << "generation: " << generation << Endl;
        OvertakenBuffer.Find(realHead, [&](ui64 idx) {
            std::atomic<ui64> &realCurrentSlot = Buffer[idx];
            ui64 realValue = realCurrentSlot.load(std::memory_order_acquire);
            TSlot realSlot = TSlot::Recognise(realValue);
            if (!realSlot.IsEmpty) {
                if (realCurrentSlot.compare_exchange_strong(realValue, TSlot::MakeEmpty(generation + 1))) {
                    // Cerr << "Changed generation at " << ConvertIdx(idx) << " to " << generation << Endl;
                    result = realSlot.Value;
                    return true;
                }
            }
            return false;
        });
        return result;
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
        // Cerr << "TryPop\n";
        for (ui32 it = 0;; ++it) {
            OBSERVE_WITH_CONDITION(LongFastPop10It, it == 10);
            OBSERVE_WITH_CONDITION(LongFastPop100It, it == 100);
            OBSERVE_WITH_CONDITION(LongFastPop1000It, it == 1000);

            ui64 prevHead = Head.load(std::memory_order_acquire);
            if (prevHead & HasOvertakenSlotsBit) {
                if (auto value = TryPopFromOvertakenSlots(prevHead ^ HasOvertakenSlotsBit)) {
                    OBSERVE(SuccessOvertakenPop);
                    return value;
                }
                prevHead = Head.load(std::memory_order_acquire) & ~HasOvertakenSlotsBit;
                ui64 currentTail = Tail.load(std::memory_order_acquire) & ~HasOvertakenSlotsBit;;
                if (prevHead >= currentTail) {
                    OBSERVE(FailedSlowPop);
                    return std::nullopt;
                }
            }

            ui64 currentHead = Head.fetch_add(1, std::memory_order_relaxed) & ~HasOvertakenSlotsBit;;
            OBSERVE(AfterReserveSlotInFastPop);
            ui32 generation = currentHead / MaxSize;

            ui64 slotIdx = ConvertIdx(currentHead);
            std::atomic<ui64> &currentSlot = Buffer[slotIdx];

            ui64 expected = currentSlot.load(std::memory_order_relaxed);
            TSlot slot = TSlot::Recognise(expected);

            bool skipIteration = false;
            while (generation >= slot.Generation) {
                if (slot.IsEmpty) {
                    if (currentSlot.compare_exchange_weak(expected, TSlot::MakeOvertaken(generation))) {
                        // Cerr << "Changed generation at " << currentHead % MaxSize << " to " << generation << Endl;
                        if (!OvertakenBuffer.Push(slotIdx)) {
                            if (auto value = InvalidateSlot(currentSlot, generation)) {
                                OBSERVE(SuccessFastPop);
                                return value;
                            }
                        } else {
                            while (!(currentHead & HasOvertakenSlotsBit)) {
                                if (Head.compare_exchange_strong(currentHead, currentHead | HasOvertakenSlotsBit, std::memory_order_acq_rel)) {
                                    break;
                                }
                            }
                        }
                        skipIteration = true;
                        break;
                    }
                } else if (slot.IsOvertaken) {
                    skipIteration = true;
                    break;
                } else {
                    if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                        // Cerr << "Changed generation at " << currentHead % MaxSize << " to " << generation + 1 << Endl;
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

            ui64 currentTail = Tail.load(std::memory_order_acquire)  & ~HasOvertakenSlotsBit;;
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