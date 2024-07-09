#pragma once
#include "defs.h"

#include "observer/methods.h"

#include <library/cpp/threading/chunk_queue/queue.h>
#include <util/system/mutex.h>

#include <atomic>
#include <optional>

namespace NActors {

template <ui32 MaxSizeBits, typename TObserver=void>
struct TMPMCRingQueueV1 {

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

    static constexpr ui64 PushStopBit = 1ull << 63;
    static constexpr ui64 HasOvertakenSlotsBit = 1ull << 63;
    NThreading::TPadded<std::atomic<ui64>> Tail{0};
    NThreading::TPadded<std::atomic<ui64>> Head{0};
    NThreading::TPadded<TArrayHolder<std::atomic<ui64>>> Buffer;
    static constexpr ui64 OvertakenSlotBufferSize = 4'096;
    NThreading::TPadded<std::atomic<ui64>> OvertakenHeadTail{0};
    NThreading::TPadded<TArrayHolder<std::atomic<ui64>>> OvertakenSlotBuffer;
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

    TMPMCRingQueueV1()
        : Buffer(new std::atomic<ui64>[MaxSize])
        , OvertakenSlotBuffer(new std::atomic<ui64>[OvertakenSlotBufferSize])
    {
        for (ui32 idx = 0; idx < MaxSize; ++idx) {
            Buffer[idx] = TSlot::MakeEmpty(0);
        }
        for (ui32 idx = 0; idx < OvertakenSlotBufferSize; ++idx) {
            OvertakenSlotBuffer[idx] = TSlot::MakeEmpty(0);
        }
    }

    bool TryPush(ui32 val) {
        for (ui32 it = 0;; ++it) {
            OBSERVE_WITH_CONDITION(LongPush10It, it == 10);
            OBSERVE_WITH_CONDITION(LongPush100It, it == 100);
            OBSERVE_WITH_CONDITION(LongPush1000It, it == 1000);
            ui64 currentTail = Tail.fetch_add(1, std::memory_order_relaxed);
            OBSERVE(AfterReserveSlotInFastPush);

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentTail)];
            TSlot slot;
            ui64 expected = TSlot::MakeEmpty(0);
            do {
                if (currentSlot.compare_exchange_strong(expected, val, std::memory_order_acq_rel)) {
                    OBSERVE(SuccessFastPush);
                    return true;
                }
                slot = TSlot::Recognise(expected);
            } while (slot.IsEmpty);

            if (!slot.IsEmpty) {
                ui64 currentHead = Head.load(std::memory_order_acquire) & ~HasOvertakenSlotsBit;
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

    std::optional<ui32> TryPopFromOvertakenSlots(ui64 realHead) {
        ui64 headTail = OvertakenHeadTail.load(std::memory_order_acquire);
        ui32 head = headTail >> 32;
        ui32 tail = headTail & ((1ull << 32) - 1);
        while (head == tail) {
            if (Head.compare_exchange_strong(realHead, realHead & ~HasOvertakenSlotsBit, std::memory_order_acq_rel)) {
                return std::nullopt;
            }
            headTail = OvertakenHeadTail.load(std::memory_order_acquire);
            head = headTail >> 32;
            tail = headTail & ((1ull << 32) - 1);
        }
        TSlot prevSlot = TSlot{.IsEmpty = true};
        ui64 prevSlotIdx = 0;
        ui64 prevValue = 0;
        for (ui32 idx = head; idx < tail; ++idx) {
            ui64 slotIdx = ConvertOvertakenIdx(idx);
            std::atomic<ui64> &currentSlot = OvertakenSlotBuffer[slotIdx];
            ui64 value = currentSlot.load(std::memory_order_acquire);
            TSlot slot = TSlot::Recognise(value);

            if (!slot.IsEmpty) {
                std::atomic<ui64> &realCurrentSlot = Buffer[slot.Value];
                ui64 realValue = realCurrentSlot.load(std::memory_order_acquire);
                TSlot realSlot = TSlot::Recognise(realValue);
                if (realSlot.IsOvertaken) {
                    prevSlot = slot;
                    prevSlotIdx = slotIdx;
                    prevValue = value;
                    continue;
                }
                if (!realSlot.IsEmpty) {
                    if (realCurrentSlot.compare_exchange_strong(realValue, TSlot::MakeEmpty(0))) {
                        ui64 generation = idx / OvertakenSlotBufferSize;
                        currentSlot.compare_exchange_strong(value, TSlot::MakeEmpty(generation + 1), std::memory_order_acq_rel);
                        return realSlot.Value;
                    }
                } else {
                    ui64 generation = idx / OvertakenSlotBufferSize;
                    currentSlot.compare_exchange_strong(value, TSlot::MakeEmpty(generation + 1), std::memory_order_acq_rel);
                    value = TSlot::MakeEmpty(generation + 1);
                    slot.IsEmpty = true;
                }
            }

            if (slot.IsEmpty) {
                if (idx == head) {
                    do {
                        if (OvertakenHeadTail.compare_exchange_strong(headTail, headTail + (1ull << 32), std::memory_order_acq_rel)) {
                            break;
                        }
                        head = headTail >> 32;
                        tail = tail & ((1ull << 32) - 1);
                    } while (idx == head);

                    if (idx + 1 < head) {
                        idx = head - 1;
                    }
                } else if (!prevSlot.IsEmpty) {
                    currentSlot.store(prevValue, std::memory_order_release);
                    ui64 generation = idx / OvertakenSlotBufferSize;
                    OvertakenSlotBuffer[prevSlotIdx].compare_exchange_strong(prevValue, TSlot::MakeEmpty(generation + 1), std::memory_order_acq_rel);
                    prevSlotIdx = slotIdx;
                }
            }

        }
        return std::nullopt;
    }

    void AddOvertakenSlot(ui64 slotIdx) {
        for (;;) {
            ui64 currentTail = OvertakenHeadTail.fetch_add(1, std::memory_order_relaxed);
            ui32 generation = currentTail / MaxSize;

            std::atomic<ui64> &currentSlot = OvertakenSlotBuffer[ConvertOvertakenIdx(currentTail)];
            TSlot slot;
            ui64 expected = TSlot::MakeEmpty(generation);
            do {
                if (currentSlot.compare_exchange_strong(expected, slotIdx, std::memory_order_acq_rel)) {
                    return;
                }
                slot = TSlot::Recognise(expected);
            } while (slot.IsEmpty && generation >= slot.Generation);

            SpinLockPause();
        }
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
        for (ui32 it = 0;; ++it) {
            OBSERVE_WITH_CONDITION(LongFastPop10It, it == 10);
            OBSERVE_WITH_CONDITION(LongFastPop100It, it == 100);
            OBSERVE_WITH_CONDITION(LongFastPop1000It, it == 1000);

            ui64 prevHead = Head.load(std::memory_order_acquire);
            if (prevHead & HasOvertakenSlotsBit) {
                if (auto value = TryPopFromOvertakenSlots(prevHead & ~HasOvertakenSlotsBit)) {
                    OBSERVE(SuccessOvertakenPop);
                    return value;
                }
                prevHead = Head.load(std::memory_order_acquire) & ~HasOvertakenSlotsBit;
                ui64 currentTail = Tail.load(std::memory_order_acquire);
                if (prevHead >= currentTail) {
                    OBSERVE(FailedSlowPop);
                    return std::nullopt;
                }
            }

            ui64 currentHead = Head.fetch_add(1, std::memory_order_relaxed) & ~HasOvertakenSlotsBit;
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
                        AddOvertakenSlot(slotIdx);
                        while (!(currentHead & HasOvertakenSlotsBit)) {
                            if (Head.compare_exchange_strong(currentHead, currentHead | HasOvertakenSlotsBit, std::memory_order_acq_rel)) {
                                break;
                            }
                        }
                        skipIteration = true;
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