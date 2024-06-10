#pragma once
#include "defs.h"

#include <library/cpp/threading/chunk_queue/queue.h>
#include <util/system/mutex.h>

#include <atomic>
#include <optional>

namespace NActors {

struct TMPMCRingQueueStats {

    struct TStats {
        ui64 SuccessPush = 0;
        ui64 SuccessSlowPush = 0;
        ui64 SuccessFastPush = 0;

        ui64 FailedPush = 0;
        ui64 FailedSlowPushAttempts = 0;

        ui64 ChangesFastPushToSlowPush = 0;
        ui64 ChangesReallySlowPopToSlowPop = 0;

        ui64 SuccessPops = 0;
        ui64 SuccessSingleConsumerPops = 0;
        ui64 SuccessSlowPops = 0;
        ui64 SuccessFastPops = 0;
        ui64 SuccessReallyFastPops = 0;

        ui64 FailedPops = 0;
        ui64 FailedSingleConsumerPops = 0;
        ui64 FailedSingleConsumerPopAttempts = 0;
        ui64 FailedReallySlowPops = 0;
        ui64 FailedSlowPops = 0;
        ui64 FailedSlowPopAttempts = 0;
        ui64 FailedFastPops = 0;
        ui64 FailedFastPopAttempts = 0;
        ui64 FailedReallyFastPops = 0;
        ui64 FailedReallyFastPopAttempts = 0;


        ui64 InvalidatedSlots = 0;
        ui64 InvalidatedSlotsInSlowPop = 0;
        ui64 InvalidatedSlotsInFastPop = 0;
        ui64 InvalidatedSlotsInReallyFastPop = 0;

        ui64 MoveTailBecauseHeadOvertakesInReallySlowPop = 0;
        ui64 MoveTailBecauseHeadOvertakesInSlowPop = 0;
        ui64 MoveTailBecauseHeadOvertakesInFastPop = 0;

        ui64 FoundOldSlot = 0;
        ui64 FoundOldSlotInSlowPush = 0;
        ui64 FoundOldSlotInFastPush = 0;
        ui64 FoundOldSlotInSlowPop = 0;
        ui64 FoundOldSlotInFastPop = 0;
        ui64 FoundOldSlotInReallyFastPop = 0;

        ui64 LongPush = 0;
        ui64 LongSlowPop = 0;
        ui64 LongFastPop = 0;
        ui64 LongReallyFastPop = 0;

        TStats& operator += (const TStats &other) {
            SuccessPush += other.SuccessPush;
            SuccessSlowPush += other.SuccessSlowPush;
            SuccessFastPush += other.SuccessFastPush;
            ChangesFastPushToSlowPush += other.ChangesFastPushToSlowPush;
            ChangesReallySlowPopToSlowPop += other.ChangesReallySlowPopToSlowPop;
            FailedPush += other.FailedPush;
            FailedSlowPushAttempts += other.FailedSlowPushAttempts;

            SuccessPops += other.SuccessPops;
            SuccessSingleConsumerPops += other.SuccessSingleConsumerPops;
            SuccessSlowPops += other.SuccessSlowPops;
            SuccessFastPops += other.SuccessFastPops;
            SuccessReallyFastPops += other.SuccessReallyFastPops;

            FailedPops += other.FailedPops;
            FailedSingleConsumerPops += other.FailedSingleConsumerPops;
            FailedSingleConsumerPopAttempts += other.FailedSingleConsumerPopAttempts;
            FailedReallySlowPops += other.FailedReallySlowPops;
            FailedSlowPops += other.FailedSlowPops;
            FailedSlowPopAttempts += other.FailedSlowPopAttempts;
            FailedFastPops += other.FailedFastPops;
            FailedFastPopAttempts += other.FailedFastPopAttempts;
            FailedReallyFastPops += other.FailedReallyFastPops;
            FailedReallyFastPopAttempts += other.FailedReallyFastPopAttempts;

            MoveTailBecauseHeadOvertakesInReallySlowPop += other.MoveTailBecauseHeadOvertakesInReallySlowPop;
            MoveTailBecauseHeadOvertakesInSlowPop += other.MoveTailBecauseHeadOvertakesInSlowPop;
            MoveTailBecauseHeadOvertakesInFastPop += other.MoveTailBecauseHeadOvertakesInFastPop;

            InvalidatedSlots += other.InvalidatedSlots;
            InvalidatedSlotsInSlowPop += other.InvalidatedSlotsInSlowPop;
            InvalidatedSlotsInFastPop += other.InvalidatedSlotsInFastPop;
            InvalidatedSlotsInReallyFastPop += other.InvalidatedSlotsInReallyFastPop;

            FoundOldSlot += other.FoundOldSlot;
            FoundOldSlotInSlowPush += other.FoundOldSlotInSlowPush;
            FoundOldSlotInFastPush += other.FoundOldSlotInFastPush;
            FoundOldSlotInSlowPop += other.FoundOldSlotInSlowPop;
            FoundOldSlotInFastPop += other.FoundOldSlotInFastPop;
            FoundOldSlotInReallyFastPop += other.FoundOldSlotInReallyFastPop;

            LongPush += other.LongPush;
            LongSlowPop += other.LongSlowPop;
            LongFastPop += other.LongFastPop;
            LongReallyFastPop += other.LongReallyFastPop;

            return *this;
        }
    };
    static thread_local TStats Stats;

    template <typename  ...TArgs>
    static void IncrementMetrics(TArgs& ...args) {
        if constexpr (CollectStatistics) {
            auto dummy = [](...) {};
            dummy(++args...);
        }
    }

#define DEFINE_INCREMENT_STATS_1(M1)                \
    static void Mark ## M1() {                      \
        IncrementMetrics(Stats.M1);                 \
    }                                               \
// end DEFINE_INCREMENT_STATS_1

#define DEFINE_INCREMENT_STATS_2(M1, M2)            \
    static void Mark ## M1() {                      \
        IncrementMetrics(Stats.M1, Stats.M2);       \
    }                                               \
// end DEFINE_INCREMENT_STATS_2

    DEFINE_INCREMENT_STATS_2(SuccessSlowPush, SuccessPush)
    DEFINE_INCREMENT_STATS_2(SuccessFastPush, SuccessPush)
    DEFINE_INCREMENT_STATS_1(ChangesFastPushToSlowPush)
    DEFINE_INCREMENT_STATS_1(FailedPush)
    DEFINE_INCREMENT_STATS_1(FailedSlowPushAttempts)
    DEFINE_INCREMENT_STATS_2(SuccessSingleConsumerPops, SuccessPops)
    DEFINE_INCREMENT_STATS_2(FailedSingleConsumerPops, FailedPops)
    DEFINE_INCREMENT_STATS_1(FailedSingleConsumerPopAttempts)
    DEFINE_INCREMENT_STATS_1(MoveTailBecauseHeadOvertakesInReallySlowPop)
    DEFINE_INCREMENT_STATS_2(FailedReallySlowPops, FailedPops)
    DEFINE_INCREMENT_STATS_2(SuccessSlowPops, SuccessPops)
    DEFINE_INCREMENT_STATS_2(FailedSlowPops, FailedPops)
    DEFINE_INCREMENT_STATS_1(FailedSlowPopAttempts)
    DEFINE_INCREMENT_STATS_1(ChangesReallySlowPopToSlowPop)
    DEFINE_INCREMENT_STATS_2(SuccessFastPops, SuccessPops)
    DEFINE_INCREMENT_STATS_2(FailedFastPops, FailedPops)
    DEFINE_INCREMENT_STATS_1(MoveTailBecauseHeadOvertakesInFastPop)
    DEFINE_INCREMENT_STATS_1(MoveTailBecauseHeadOvertakesInSlowPop)
    DEFINE_INCREMENT_STATS_1(FailedFastPopAttempts)
    DEFINE_INCREMENT_STATS_2(SuccessReallyFastPops, SuccessPops)
    DEFINE_INCREMENT_STATS_2(FailedReallyFastPops, FailedPops)
    DEFINE_INCREMENT_STATS_1(FailedReallyFastPopAttempts)
    DEFINE_INCREMENT_STATS_2(InvalidatedSlotsInSlowPop, InvalidatedSlots)
    DEFINE_INCREMENT_STATS_2(InvalidatedSlotsInFastPop, InvalidatedSlots)
    DEFINE_INCREMENT_STATS_2(InvalidatedSlotsInReallyFastPop, InvalidatedSlots)
    
    DEFINE_INCREMENT_STATS_2(FoundOldSlotInSlowPush, FoundOldSlot)
    DEFINE_INCREMENT_STATS_2(FoundOldSlotInFastPush, FoundOldSlot)
    DEFINE_INCREMENT_STATS_2(FoundOldSlotInSlowPop, FoundOldSlot)
    DEFINE_INCREMENT_STATS_2(FoundOldSlotInFastPop, FoundOldSlot)
    DEFINE_INCREMENT_STATS_2(FoundOldSlotInReallyFastPop, FoundOldSlot)

    DEFINE_INCREMENT_STATS_1(LongPush)
    DEFINE_INCREMENT_STATS_1(LongSlowPop)
    DEFINE_INCREMENT_STATS_1(LongFastPop)
    DEFINE_INCREMENT_STATS_1(LongReallyFastPop)

#undef DEFINE_INCREMENT_STATS_1
#undef DEFINE_INCREMENT_STATS_2

    static TStats GetLocalStats() {
        return Stats;
    }
};

#define DEFINE_HAS_STATIC_METHOD_CONCEPT(STATIC_METHOD)   \
    template <typename T>                                 \
    concept HasStaticMethod ## STATIC_METHOD = requires { \
        { T:: STATIC_METHOD } -> std::same_as<void(&)()>; \
    };                                                    \
// DEFINE_HAS_STATIC_METHOD_CONCEPT
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkSuccessSlowPush)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkSuccessFastPush)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkChangesFastPushToSlowPush)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkFailedPush)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkFailedSlowPushAttempts)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkSuccessSingleConsumerPops)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkFailedSingleConsumerPops)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkFailedSingleConsumerPopAttempts)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkMoveTailBecauseHeadOvertakesInReallySlowPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkFailedReallySlowPops)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkSuccessSlowPops)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkFailedSlowPops)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkFailedSlowPopAttempts)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkMoveTailBecauseHeadOvertakesInSlowPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkChangesReallySlowPopToSlowPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkSuccessFastPops)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkFailedFastPops)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkMoveTailBecauseHeadOvertakesInFastPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkFailedFastPopAttempts)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkSuccessReallyFastPops)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkFailedReallyFastPops)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkFailedReallyFastPopAttempts)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkInvalidatedSlotsInSlowPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkInvalidatedSlotsInFastPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkInvalidatedSlotsInReallyFastPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkFoundOldSlotInSlowPush)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkFoundOldSlotInFastPush)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkFoundOldSlotInSlowPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkFoundOldSlotInFastPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkFoundOldSlotInReallyFastPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkLongPush)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkLongSlowPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkLongFastPop)
    DEFINE_HAS_STATIC_METHOD_CONCEPT(MarkLongReallyFastPop)
#undef DEFINE_HAS_STATIC_METHOD_CONCEPT

template <ui32 MaxSizeBits, typename TStats=void>
struct TMPMCRingQueue {

#define HAS_STAT(STAT_NAME) \
    HasStaticMethodMark ## STAT_NAME <TStats>

#define MARK_STAT_WITH_CONDITION(STAT_NAME, CONDITION) \
    if constexpr (HAS_STAT(STAT_NAME)) {               \
        if ((CONDITION)) {                             \
            TStats::Mark ## STAT_NAME ();              \
        }                                              \
    } do {} while (false)                              \
// end MARK_STAT_WITH_CONDITION

#define MARK_STAT(STAT_NAME) \
    MARK_STAT_WITH_CONDITION(STAT_NAME, true)
 
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
            return (1ull << 63) | generation;
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
            MARK_STAT_WITH_CONDITION(LongPush, it == 10);
            ui32 generation = currentTail / MaxSize;

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentTail)];
            TSlot slot;
            ui64 expected = TSlot::MakeEmpty(generation);
            do {
                if (currentSlot.compare_exchange_weak(expected, val)) {
                    Tail.compare_exchange_strong(currentTail, currentTail + 1);
                    MARK_STAT(SuccessSlowPush);
                    MARK_STAT_WITH_CONDITION(FoundOldSlotInSlowPush, (slot = TSlot::Recognise(expected), slot.Generation < generation));
                    return true;
                }
                slot = TSlot::Recognise(expected);
            } while (slot.Generation <= generation && slot.IsEmpty);

            if (!slot.IsEmpty) {
                ui64 currentHead = Head.load(std::memory_order_acquire);
                if (currentHead + MaxSize <= currentTail + std::min<ui64>(64, MaxSize - 1)) {
                    MARK_STAT(FailedPush);
                    return false;
                }
            }

            SpinLockPause();
            MARK_STAT(FailedSlowPushAttempts);
            Tail.compare_exchange_strong(currentTail, currentTail + 1, std::memory_order_acq_rel);
        }
    }

    bool TryPush(ui32 val) {
        ui64 currentTail = Tail.fetch_add(1, std::memory_order_relaxed);
        ui32 generation = currentTail / MaxSize;

        std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentTail)];
        TSlot slot;
        ui64 expected = TSlot::MakeEmpty(generation);
        do {
            if (currentSlot.compare_exchange_weak(expected, val)) {
                MARK_STAT(SuccessFastPush);
                MARK_STAT_WITH_CONDITION(FoundOldSlotInFastPush, (slot = TSlot::Recognise(expected), slot.Generation < generation));
                return true;
            }
            slot = TSlot::Recognise(expected);
        } while (slot.Generation <= generation && slot.IsEmpty);

        MARK_STAT(ChangesFastPushToSlowPush);
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
                    MARK_STAT(FailedSingleConsumerPops);
                    return std::nullopt;
                }
                if (currentSlot.compare_exchange_strong(expected, TSlot::MakeEmpty(LocalGeneration + 1))) {
                    ShiftLocalHead();
                }
                MARK_STAT(FailedSingleConsumerPopAttempts);
                SpinLockPause();
                continue;
            }
            currentSlot.store(TSlot::MakeEmpty(LocalGeneration + 1), std::memory_order_release);
            ShiftLocalHead();
            MARK_STAT(SuccessSingleConsumerPops);
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
                MARK_STAT(MoveTailBecauseHeadOvertakesInReallySlowPop);
            }
        }
        if (currentHead == currentTail) {
            MARK_STAT(FailedReallySlowPops);
            return std::nullopt;
        }

        MARK_STAT(ChangesReallySlowPopToSlowPop);
        return TryPopSlow(currentHead);
    }

    std::optional<ui32> TryPopSlow(ui64 currentHead) {
        if (!currentHead) {
            currentHead = Head.load(std::memory_order_acquire);
        }
        for (ui32 it = 0;; ++it) {
            MARK_STAT_WITH_CONDITION(LongSlowPop, it == 10);
            ui32 generation = currentHead / MaxSize;

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentHead)];

            ui64 expected = currentSlot.load(std::memory_order_relaxed);
            TSlot slot = TSlot::Recognise(expected);

            if (slot.Generation > generation) {
                Head.compare_exchange_strong(currentHead, currentHead + 1);
                MARK_STAT(FailedSlowPopAttempts);
                SpinLockPause();
                continue;
            }

            while (generation > slot.Generation) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    if (!slot.IsEmpty) {
                        Head.compare_exchange_strong(currentHead, currentHead + 1);
                        MARK_STAT(SuccessSlowPops);
                        return slot.Value;
                    }
                    break;
                }
                slot = TSlot::Recognise(expected);
            }

            while (!slot.IsEmpty) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    Head.compare_exchange_strong(currentHead, currentHead + 1);
                    MARK_STAT(SuccessSlowPops);
                    return slot.Value;
                }
                slot = TSlot::Recognise(expected);
            }

            if (slot.Generation > generation) {
                Head.compare_exchange_strong(currentHead, currentHead + 1);
                MARK_STAT(FailedSlowPopAttempts);
                SpinLockPause();
                continue;
            }

            ui64 currentTail = Tail.load(std::memory_order_acquire);
            if (currentTail <= currentHead) {
                MARK_STAT(FailedSlowPops);
                return std::nullopt;
            }

            MARK_STAT_WITH_CONDITION(FoundOldSlotInSlowPop, slot.Generation < generation); 
            while (slot.Generation <= generation && slot.IsEmpty) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    Head.compare_exchange_strong(currentHead, currentHead + 1);
                    MARK_STAT(InvalidatedSlotsInSlowPop);
                    MARK_STAT(MoveTailBecauseHeadOvertakesInSlowPop);
                    break;
                }
                slot = TSlot::Recognise(expected);
            }

            MARK_STAT(FailedSlowPopAttempts);
            SpinLockPause();
            currentHead = Head.load(std::memory_order_acquire);
        }
        MARK_STAT(FailedSlowPops);
        return std::nullopt;
    }

    std::optional<ui32> TryPopSlow() {
        return TryPopSlow(0);
    }

    std::optional<ui32> TryPopFast() {
        for (ui32 it = 0;; ++it) {
            MARK_STAT_WITH_CONDITION(LongFastPop, it == 10);
            ui64 currentHead = Head.fetch_add(1, std::memory_order_relaxed);
            ui32 generation = currentHead / MaxSize;

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentHead)];

            ui64 expected = currentSlot.load(std::memory_order_relaxed);
            TSlot slot = TSlot::Recognise(expected);

            if (slot.Generation > generation) {
                MARK_STAT(FailedFastPopAttempts);
                SpinLockPause();
                continue;
            }

            while (generation >= slot.Generation) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    if (!slot.IsEmpty) {
                        MARK_STAT(SuccessFastPops);
                        return slot.Value;
                    }
                    break;
                }
                slot = TSlot::Recognise(expected);
            }

            if (slot.Generation > generation) {
                MARK_STAT(FailedFastPopAttempts);
                SpinLockPause();
                continue;
            }

            MARK_STAT_WITH_CONDITION(FoundOldSlotInFastPop, slot.Generation < generation); 
            MARK_STAT_WITH_CONDITION(InvalidatedSlotsInFastPop, slot.Generation <= generation); 

            ui64 currentTail = Tail.load(std::memory_order_acquire);
            if (currentTail > currentHead) {
                MARK_STAT(FailedFastPopAttempts);
                SpinLockPause();
                continue;
            }

            while (currentTail <= currentHead) {
                if (Tail.compare_exchange_weak(currentTail, currentHead + 1)) {
                    MARK_STAT(FailedFastPops);
                    return std::nullopt;
                }
            }

            SpinLockPause();
        }
    }

    std::optional<ui32> TryPopReallyFast() {
        for (ui32 it = 0;; ++it) {
            MARK_STAT_WITH_CONDITION(LongReallyFastPop, it == 10);
            ui64 currentHead = Head.fetch_add(1, std::memory_order_relaxed);
            ui32 generation = currentHead / MaxSize;

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentHead)];

            ui64 expected = currentSlot.exchange(TSlot::MakeEmpty(generation + 1), std::memory_order_acq_rel);
            TSlot slot = TSlot::Recognise(expected);
            if (!slot.IsEmpty) {
                MARK_STAT(SuccessReallyFastPops);
                return slot.Value;
            }

            if (slot.Generation > generation) {
                expected = TSlot::MakeEmpty(generation + 1);
                TSlot slot2 = TSlot::Recognise(expected);
                while (slot.Generation > slot2.Generation) {
                    if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(slot.Generation))) {
                        if (!slot2.IsEmpty) {
                            MARK_STAT(SuccessReallyFastPops);
                            return slot2.Value;
                        }
                        break;
                    }
                    slot2 = TSlot::Recognise(expected);
                }

                MARK_STAT(FailedReallyFastPopAttempts);
                SpinLockPause();
                continue;
            }

            MARK_STAT_WITH_CONDITION(FoundOldSlotInReallyFastPop, slot.Generation < generation); 
            MARK_STAT_WITH_CONDITION(InvalidatedSlotsInReallyFastPop, slot.Generation <= generation); 

            if (slot.Generation > generation) {
                MARK_STAT(FailedReallyFastPopAttempts);
                SpinLockPause();
                continue;
            }

            ui64 currentTail = Tail.load(std::memory_order_acquire);
            while (currentTail <= currentHead) {
                if (Tail.compare_exchange_weak(currentTail, currentHead + 1)) {
                    MARK_STAT(FailedReallyFastPops);
                    return std::nullopt;
                }
            }

            SpinLockPause();
        }
    }
#undef MARK_STAT_WITH_CONDITION
#undef MARK_STAT
#undef HAS_STAT
};

}  // NActors