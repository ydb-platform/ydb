#pragma once
#include "defs.h"

#include "observer/methods.h"

#include <library/cpp/threading/chunk_queue/queue.h>
#include <util/system/mutex.h>

#include <atomic>
#include <optional>

namespace NActors {

template <ui32 MaxSizeBits, typename TObserver=void>
struct TMPMCRingQueue {

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

    enum class EPopMode {
        Fast,
        ReallyFast,
        Slow,
        ReallySlow,
    };

    struct alignas(ui64) TSlot {
        static constexpr ui64 EmptyBit = 1ull << 63;
        ui64 Generation = 0;
        ui64 Value = 0;
        bool IsEmpty;

        static constexpr ui64 MakeEmpty(ui64 generation) {
            return EmptyBit | generation;
        }

        static constexpr TSlot Recognise(ui64 slotValue) {
            if (slotValue & EmptyBit) {
                return {.Generation = (EmptyBit ^ slotValue), .IsEmpty=true};
            }
            return {.Value=slotValue, .IsEmpty=false};
        }
    };

    NThreading::TPadded<std::atomic<ui64>> Tail{0};
    NThreading::TPadded<std::atomic<ui64>> Head{0};
    NThreading::TPadded<TArrayHolder<std::atomic<ui64>>> Buffer;
    ui64 LocalHead = 0;
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

    TMPMCRingQueue()
        : Buffer(new std::atomic<ui64>[MaxSize])
    {
        for (ui32 idx = 0; idx < MaxSize; ++idx) {
            Buffer[idx] = TSlot::MakeEmpty(0);
        }
    }

    bool TryPushSlow(ui32 val) {
        ui64 currentTail = Tail.load(std::memory_order_relaxed);
        for (ui32 it = 0;; ++it) {
            OBSERVE_WITH_CONDITION(LongPush10It, it == 10);
            OBSERVE_WITH_CONDITION(LongPush100It, it == 100);
            OBSERVE_WITH_CONDITION(LongPush1000It, it == 1000);
            ui32 generation = currentTail / MaxSize;

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentTail)];
            TSlot slot;
            ui64 expected = TSlot::MakeEmpty(generation);
            do {
                if (currentSlot.compare_exchange_weak(expected, val)) {
                    Tail.compare_exchange_strong(currentTail, currentTail + 1);
                    OBSERVE(SuccessSlowPush);
                    OBSERVE_WITH_CONDITION(FoundOldSlotInSlowPush, (slot = TSlot::Recognise(expected), slot.Generation < generation));
                    return true;
                }
                slot = TSlot::Recognise(expected);
            } while (slot.Generation <= generation && slot.IsEmpty);

            if (!slot.IsEmpty) {
                ui64 currentHead = Head.load(std::memory_order_acquire);
                if (currentHead + MaxSize <= currentTail + std::min<ui64>(64, MaxSize - 1)) {
                    OBSERVE(FailedPush);
                    return false;
                }
            }

            SpinLockPause();
            OBSERVE(FailedSlowPushAttempt);
            Tail.compare_exchange_strong(currentTail, currentTail + 1, std::memory_order_acq_rel);
        }
    }

    bool TryPush(ui32 val) {
        ui64 currentTail = Tail.fetch_add(1, std::memory_order_relaxed);
        OBSERVE(AfterReserveSlotInFastPush);
        ui32 generation = currentTail / MaxSize;

        std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentTail)];
        TSlot slot;
        ui64 expected = TSlot::MakeEmpty(generation);
        do {
            if (currentSlot.compare_exchange_weak(expected, val)) {
                OBSERVE(SuccessFastPush);
                OBSERVE_WITH_CONDITION(FoundOldSlotInFastPush, (slot = TSlot::Recognise(expected), slot.Generation < generation));
                return true;
            }
            slot = TSlot::Recognise(expected);
        } while (slot.Generation <= generation && slot.IsEmpty);

        OBSERVE(ChangeFastPushToSlowPush);
        return TryPushSlow(val);
    }

    void ShiftLocalHead() {
        if (++LocalHead == MaxSize) {
            LocalHead = 0;
            LocalGeneration++;
        }
    }

    std::optional<ui32> TryPopSingleConsumer() {
        for (;;) {
            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(LocalHead)];
            ui64 expected = currentSlot.load(std::memory_order_acquire);
            TSlot slot = TSlot::Recognise(expected);
            if (slot.IsEmpty) {
                ui64 currentTail = Tail.load(std::memory_order_acquire);
                ui64 globalHead = LocalGeneration * MaxSize + LocalHead;
                if (currentTail <= globalHead) {
                    Tail.compare_exchange_strong(currentTail, globalHead);
                    OBSERVE(FailedSingleConsumerPop);
                    return std::nullopt;
                }
                if (currentSlot.compare_exchange_strong(expected, TSlot::MakeEmpty(LocalGeneration + 1))) {
                    ShiftLocalHead();
                }
                OBSERVE(FailedSingleConsumerPopAttempt);
                SpinLockPause();
                continue;
            }
            currentSlot.store(TSlot::MakeEmpty(LocalGeneration + 1), std::memory_order_release);
            ShiftLocalHead();
            OBSERVE(SuccessSingleConsumerPop);
            return slot.Value;
        }
    }

    std::optional<ui32> TryPop(EPopMode &mode) {
        switch (mode) {
        case EPopMode::Fast:
            if (auto item = TryPopFast()) {
                mode = EPopMode::ReallyFast;
                return item;
            }
            mode = EPopMode::Slow;
            return std::nullopt;
        case EPopMode::ReallyFast:
            if (auto item = TryPopReallyFast()) {
                return item;
            }
            mode = EPopMode::Slow;
            return std::nullopt;
        case EPopMode::Slow:
            if (auto item = TryPopSlow()) {
                mode = EPopMode::Fast;
                return item;
            }
            mode = EPopMode::ReallySlow;
            return std::nullopt;
        case EPopMode::ReallySlow:
            if (auto item = TryPopReallySlow()) {
                mode = EPopMode::Fast;
                return item;
            }
            return std::nullopt;
        }
    }

    void TryIncrementHead(ui64 &currentHead) {
        ui64 expectedHead = currentHead;
        while (expectedHead <= currentHead) {
            if (Head.compare_exchange_weak(expectedHead, currentHead + 1)) {
                currentHead++;
                return;
            }
        }
        currentHead = expectedHead;
    }

    std::optional<ui32> TryPopReallySlow() {
        ui64 currentHead = Head.load(std::memory_order_acquire);
        ui64 currentTail = Tail.load(std::memory_order_acquire);
        while (currentHead > currentTail) {
            if (Tail.compare_exchange_weak(currentTail, currentHead)) {
                currentTail = currentHead;
                OBSERVE(MoveTailBecauseHeadOvertakesInReallySlowPop);
            }
        }
        if (currentHead == currentTail) {
            OBSERVE(FailedReallySlowPop);
            return std::nullopt;
        }

        OBSERVE(ChangeReallySlowPopToSlowPop);
        return TryPopSlow(currentHead);
    }

    std::optional<ui32> TryPopSlow(ui64 currentHead) {
        if (!currentHead) {
            currentHead = Head.load(std::memory_order_acquire);
        }
        for (ui32 it = 0;; ++it) {
            OBSERVE_WITH_CONDITION(LongSlowPop10It, it == 10);
            OBSERVE_WITH_CONDITION(LongSlowPop100It, it == 100);
            OBSERVE_WITH_CONDITION(LongSlowPop1000It, it == 1000);
            ui32 generation = currentHead / MaxSize;

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentHead)];

            ui64 expected = currentSlot.load(std::memory_order_relaxed);
            TSlot slot = TSlot::Recognise(expected);

            if (slot.Generation > generation) {
                Head.compare_exchange_strong(currentHead, currentHead + 1);
                OBSERVE(FailedSlowPopAttempt);
                SpinLockPause();
                continue;
            }

            while (generation > slot.Generation) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    if (!slot.IsEmpty) {
                        Head.compare_exchange_strong(currentHead, currentHead + 1);
                        OBSERVE(SuccessSlowPop);
                        return slot.Value;
                    }
                    break;
                }
                slot = TSlot::Recognise(expected);
            }

            while (!slot.IsEmpty) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    Head.compare_exchange_strong(currentHead, currentHead + 1);
                    OBSERVE(SuccessSlowPop);
                    return slot.Value;
                }
                slot = TSlot::Recognise(expected);
            }

            if (slot.Generation > generation) {
                Head.compare_exchange_strong(currentHead, currentHead + 1);
                OBSERVE(FailedSlowPopAttempt);
                SpinLockPause();
                continue;
            }

            ui64 currentTail = Tail.load(std::memory_order_acquire);
            if (currentTail <= currentHead) {
                OBSERVE(FailedSlowPop);
                return std::nullopt;
            }

            OBSERVE_WITH_CONDITION(FoundOldSlotInSlowPop, slot.Generation < generation); 
            while (slot.Generation <= generation && slot.IsEmpty) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    Head.compare_exchange_strong(currentHead, currentHead + 1);
                    OBSERVE(InvalidatedSlotInSlowPop);
                    OBSERVE(MoveTailBecauseHeadOvertakesInSlowPop);
                    break;
                }
                slot = TSlot::Recognise(expected);
            }

            OBSERVE(FailedSlowPopAttempt);
            SpinLockPause();
            currentHead = Head.load(std::memory_order_acquire);
        }
        OBSERVE(FailedSlowPop);
        return std::nullopt;
    }

    std::optional<ui32> TryPopSlow() {
        return TryPopSlow(0);
    }

    std::optional<ui32> TryPopFast() {
        for (ui32 it = 0;; ++it) {
            OBSERVE_WITH_CONDITION(LongFastPop10It, it == 10);
            OBSERVE_WITH_CONDITION(LongFastPop100It, it == 100);
            OBSERVE_WITH_CONDITION(LongFastPop1000It, it == 1000);
            ui64 currentHead = Head.fetch_add(1, std::memory_order_relaxed);
            OBSERVE(AfterReserveSlotInFastPop);
            ui32 generation = currentHead / MaxSize;

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentHead)];

            ui64 expected = currentSlot.load(std::memory_order_relaxed);
            TSlot slot = TSlot::Recognise(expected);

            if (slot.Generation > generation) {
                OBSERVE(FailedFastPopAttempt);
                SpinLockPause();
                continue;
            }

            while (generation >= slot.Generation) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    if (!slot.IsEmpty) {
                        OBSERVE(SuccessFastPop);
                        return slot.Value;
                    }
                    break;
                }
                slot = TSlot::Recognise(expected);
            }

            if (slot.Generation > generation) {
                OBSERVE(FailedFastPopAttempt);
                SpinLockPause();
                continue;
            }

            OBSERVE_WITH_CONDITION(FoundOldSlotInFastPop, slot.Generation < generation); 
            OBSERVE_WITH_CONDITION(InvalidatedSlotInFastPop, slot.Generation <= generation); 

            ui64 currentTail = Tail.load(std::memory_order_acquire);
            if (currentTail > currentHead) {
                OBSERVE(FailedFastPopAttempt);
                SpinLockPause();
                continue;
            }

            while (currentTail <= currentHead) {
                if (Tail.compare_exchange_weak(currentTail, currentHead + 1)) {
                    OBSERVE(FailedFastPop);
                    return std::nullopt;
                }
            }

            SpinLockPause();
        }
    }

    std::optional<ui32> TryPopReallyFast() {
        for (ui32 it = 0;; ++it) {
            OBSERVE_WITH_CONDITION(LongReallyFastPop10It, it == 10);
            OBSERVE_WITH_CONDITION(LongReallyFastPop100It, it == 100);
            OBSERVE_WITH_CONDITION(LongReallyFastPop1000It, it == 1000);
            ui64 currentHead = Head.fetch_add(1, std::memory_order_relaxed);
            OBSERVE(AfterReserveSlotInReallyFastPop);
            ui32 generation = currentHead / MaxSize;

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentHead)];

            ui64 expected = currentSlot.exchange(TSlot::MakeEmpty(generation + 1), std::memory_order_acq_rel);
            TSlot slot = TSlot::Recognise(expected);
            if (!slot.IsEmpty) {
                OBSERVE(SuccessReallyFastPop);
                return slot.Value;
            }

            if (slot.Generation > generation) {
                OBSERVE(AfterIncorectlyChangeSlotGenerationInReallyFastPop);
                expected = TSlot::MakeEmpty(generation + 1);
                TSlot slot2 = TSlot::Recognise(expected);
                while (slot.Generation > slot2.Generation) {
                    if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(slot.Generation))) {
                        if (!slot2.IsEmpty) {
                            OBSERVE(SuccessReallyFastPop);
                            return slot2.Value;
                        }
                        break;
                    }
                    slot2 = TSlot::Recognise(expected);
                    SpinLockPause();
                }

                OBSERVE(FailedReallyFastPopAttempt);
                SpinLockPause();
                continue;
            }

            OBSERVE_WITH_CONDITION(FoundOldSlotInReallyFastPop, slot.Generation < generation); 
            OBSERVE_WITH_CONDITION(InvalidatedSlotInReallyFastPop, slot.Generation <= generation); 

            if (slot.Generation > generation) {
                OBSERVE(FailedReallyFastPopAttempt);
                SpinLockPause();
                continue;
            }

            ui64 currentTail = Tail.load(std::memory_order_acquire);
            while (currentTail <= currentHead) {
                if (Tail.compare_exchange_weak(currentTail, currentHead + 1)) {
                    OBSERVE(FailedReallyFastPop);
                    return std::nullopt;
                }
            }

            SpinLockPause();
        }
    }
#undef OBSERVE_WITH_CONDITION
#undef OBSERVE
#undef HAS_OBSERVE_METHOD
};

}  // NActors