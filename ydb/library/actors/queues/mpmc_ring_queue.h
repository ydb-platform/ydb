#pragma once
#include "defs.h"

#include <library/cpp/threading/chunk_queue/queue.h>
#include <util/system/mutex.h>

#include <atomic>
#include <optional>

namespace NActors {

struct TMPMCRingQueueStats {

    struct TStats {
        ui64 SuccessPushes = 0;
        ui64 SuccessSlowPushes = 0;
        ui64 SuccessFastPushes = 0;

        ui64 FailedPushes = 0;
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
        ui64 MoveTailBecauseHeadOvertakesInFastPop = 0;

        TStats& operator += (const TStats &other) {
            SuccessPushes += other.SuccessPushes;
            SuccessSlowPushes += other.SuccessSlowPushes;
            SuccessFastPushes += other.SuccessFastPushes;
            ChangesFastPushToSlowPush += other.ChangesFastPushToSlowPush;
            ChangesReallySlowPopToSlowPop += other.ChangesReallySlowPopToSlowPop;
            FailedPushes += other.FailedPushes;
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
            MoveTailBecauseHeadOvertakesInFastPop += other.MoveTailBecauseHeadOvertakesInFastPop;

            InvalidatedSlots += other.InvalidatedSlots;
            InvalidatedSlotsInSlowPop += other.InvalidatedSlotsInSlowPop;
            InvalidatedSlotsInFastPop += other.InvalidatedSlotsInFastPop;
            InvalidatedSlotsInReallyFastPop += other.InvalidatedSlotsInReallyFastPop;

            return *this;
        }
    };
    static thread_local TStats Stats;

#ifdef MPMC_RING_QUEUE_COLLECT_STATISTICS
    static constexpr bool CollectStatistics = true;
#else
    static constexpr bool CollectStatistics = false;
#endif

    template <typename  ...TArgs>
    static void IncrementMetrics(TArgs& ...args) {
        if constexpr (CollectStatistics) {
            auto dummy = [](...) {};
            dummy(++args...);
        }
    }

#define DEFINE_INCREMENT_STATS_1(M1)                \
    static void Increment ## M1() {                 \
        IncrementMetrics(Stats.M1);                 \
    }                                               \
// end DEFINE_INCREMENT_STATS_1

#define DEFINE_INCREMENT_STATS_2(M1, M2)            \
    static void Increment ## M1() {                 \
        IncrementMetrics(Stats.M1, Stats.M2);       \
    }                                               \
// end DEFINE_INCREMENT_STATS_2

    DEFINE_INCREMENT_STATS_2(SuccessSlowPushes, SuccessPushes)
    DEFINE_INCREMENT_STATS_2(SuccessFastPushes, SuccessPushes)
    DEFINE_INCREMENT_STATS_1(ChangesFastPushToSlowPush)
    DEFINE_INCREMENT_STATS_1(FailedPushes)
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
    DEFINE_INCREMENT_STATS_1(FailedFastPopAttempts)
    DEFINE_INCREMENT_STATS_2(SuccessReallyFastPops, SuccessPops)
    DEFINE_INCREMENT_STATS_2(FailedReallyFastPops, FailedPops)
    DEFINE_INCREMENT_STATS_1(FailedReallyFastPopAttempts)
    DEFINE_INCREMENT_STATS_2(InvalidatedSlotsInSlowPop, InvalidatedSlots)
    DEFINE_INCREMENT_STATS_2(InvalidatedSlotsInFastPop, InvalidatedSlots)
    DEFINE_INCREMENT_STATS_2(InvalidatedSlotsInReallyFastPop, InvalidatedSlots)

    static TStats GetLocalStats() {
        return Stats;
    }
};

template <ui32 MaxSizeBits>
struct TMPMCRingQueue {
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
        for (;;) {
            ui32 generation = currentTail / MaxSize;

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentTail)];
            TSlot slot;
            ui64 expected = TSlot::MakeEmpty(generation);
            do {
                if (currentSlot.compare_exchange_weak(expected, val)) {
                    Tail.compare_exchange_strong(currentTail, currentTail + 1);
                    TMPMCRingQueueStats::IncrementSuccessSlowPushes();
                    return true;
                }
                slot = TSlot::Recognise(expected);
            } while (slot.Generation <= generation && slot.IsEmpty);

            if (!slot.IsEmpty) {
                ui64 currentHead = Head.load(std::memory_order_acquire);
                if (currentHead + MaxSize <= currentTail + std::min<ui64>(64, MaxSize - 1)) {
                    TMPMCRingQueueStats::IncrementFailedPushes();
                    return false;
                }
            }

            SpinLockPause();
            TMPMCRingQueueStats::IncrementFailedSlowPushAttempts();
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
                TMPMCRingQueueStats::IncrementSuccessFastPushes();
                return true;
            }
            slot = TSlot::Recognise(expected);
        } while (slot.Generation <= generation && slot.IsEmpty);

        TMPMCRingQueueStats::IncrementChangesFastPushToSlowPush();
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
                    TMPMCRingQueueStats::IncrementFailedSingleConsumerPops();
                    return std::nullopt;
                }
                if (currentSlot.compare_exchange_strong(expected, TSlot::MakeEmpty(LocalGeneration + 1))) {
                    ShiftLocalHead();
                }
                TMPMCRingQueueStats::IncrementFailedSingleConsumerPopAttempts();
                SpinLockPause();
                continue;
            }
            currentSlot.store(TSlot::MakeEmpty(LocalGeneration + 1), std::memory_order_release);
            ShiftLocalHead();
            TMPMCRingQueueStats::IncrementSuccessSingleConsumerPops();
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
                TMPMCRingQueueStats::IncrementMoveTailBecauseHeadOvertakesInReallySlowPop();
            }
        }
        if (currentHead == currentTail) {
            TMPMCRingQueueStats::IncrementFailedReallySlowPops();
            return std::nullopt;
        }

        TMPMCRingQueueStats::IncrementChangesReallySlowPopToSlowPop();
        return TryPopSlow(currentHead);
    }

    std::optional<ui32> TryPopSlow(ui64 currentHead) {
        if (!currentHead) {
            currentHead = Head.load(std::memory_order_acquire);
        }
        for (;;) {
            ui32 generation = currentHead / MaxSize;

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentHead)];

            ui64 expected = currentSlot.load(std::memory_order_relaxed);
            TSlot slot = TSlot::Recognise(expected);

            if (slot.Generation > generation) {
                Head.compare_exchange_strong(currentHead, currentHead + 1);
                TMPMCRingQueueStats::IncrementFailedSlowPopAttempts();
                SpinLockPause();
                continue;
            }

            while (generation > slot.Generation) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    if (!slot.IsEmpty) {
                        Head.compare_exchange_strong(currentHead, currentHead + 1);
                        TMPMCRingQueueStats::IncrementSuccessSlowPops();
                        return slot.Value;
                    }
                    break;
                }
                slot = TSlot::Recognise(expected);
            }

            while (!slot.IsEmpty) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    Head.compare_exchange_strong(currentHead, currentHead + 1);
                    TMPMCRingQueueStats::IncrementSuccessSlowPops();
                    return slot.Value;
                }
                slot = TSlot::Recognise(expected);
            }

            if (slot.Generation > generation) {
                Head.compare_exchange_strong(currentHead, currentHead + 1);
                TMPMCRingQueueStats::IncrementFailedSlowPopAttempts();
                SpinLockPause();
                continue;
            }

            ui64 currentTail = Tail.load(std::memory_order_acquire);
            if (currentTail <= currentHead) {
                TMPMCRingQueueStats::IncrementFailedSlowPops();
                return std::nullopt;
            }

            while (slot.Generation == generation && slot.IsEmpty) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    Head.compare_exchange_strong(currentHead, currentHead + 1);
                    TMPMCRingQueueStats::IncrementInvalidatedSlotsInSlowPop();
                    TMPMCRingQueueStats::IncrementMoveTailBecauseHeadOvertakesInFastPop();
                    break;
                }
                slot = TSlot::Recognise(expected);
            }

            TMPMCRingQueueStats::IncrementFailedSlowPopAttempts();
            SpinLockPause();
            currentHead = Head.load(std::memory_order_acquire);
        }
        TMPMCRingQueueStats::IncrementFailedSlowPops();
        return std::nullopt;
    }

    std::optional<ui32> TryPopSlow() {
        return TryPopSlow(0);
    }

    std::optional<ui32> TryPopFast() {
        for (;;) {
            ui64 currentHead = Head.fetch_add(1, std::memory_order_relaxed);
            ui32 generation = currentHead / MaxSize;

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentHead)];

            ui64 expected = currentSlot.load(std::memory_order_relaxed);
            TSlot slot = TSlot::Recognise(expected);

            if (slot.Generation > generation) {
                TMPMCRingQueueStats::IncrementFailedFastPopAttempts();
                SpinLockPause();
                continue;
            }

            while (generation >= slot.Generation) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    if (!slot.IsEmpty) {
                        TMPMCRingQueueStats::IncrementSuccessFastPops();
                        return slot.Value;
                    }
                    break;
                }
                slot = TSlot::Recognise(expected);
            }

            if (slot.Generation > generation) {
                TMPMCRingQueueStats::IncrementFailedFastPopAttempts();
                SpinLockPause();
                continue;
            }

            if constexpr (TMPMCRingQueueStats::CollectStatistics) {
                if (slot.Generation <= generation) {
                    TMPMCRingQueueStats::IncrementInvalidatedSlotsInFastPop();
                }
            }

            ui64 currentTail = Tail.load(std::memory_order_acquire);
            if (currentTail > currentHead) {
                TMPMCRingQueueStats::IncrementFailedFastPopAttempts();
                SpinLockPause();
                continue;
            }

            while (currentTail <= currentHead) {
                if (Tail.compare_exchange_weak(currentTail, currentHead + 1)) {
                    TMPMCRingQueueStats::IncrementFailedFastPops();
                    return std::nullopt;
                }
            }

            SpinLockPause();
        }
    }

    std::optional<ui32> TryPopReallyFast() {
        for (;;) {
            ui64 currentHead = Head.fetch_add(1, std::memory_order_relaxed);
            ui32 generation = currentHead / MaxSize;

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentHead)];

            ui64 expected = currentSlot.exchange(TSlot::MakeEmpty(generation + 1), std::memory_order_acq_rel);
            TSlot slot = TSlot::Recognise(expected);
            if (!slot.IsEmpty) {
                TMPMCRingQueueStats::IncrementSuccessReallyFastPops();
                return slot.Value;
            }

            if (slot.Generation > generation) {
                expected = TSlot::MakeEmpty(generation + 1);
                TSlot slot2 = TSlot::Recognise(expected);
                while (slot.Generation > slot2.Generation) {
                    if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(slot.Generation))) {
                        if (!slot2.IsEmpty) {
                            TMPMCRingQueueStats::IncrementSuccessReallyFastPops();
                            return slot2.Value;
                        }
                        break;
                    }
                    slot2 = TSlot::Recognise(expected);
                }

                TMPMCRingQueueStats::IncrementFailedReallyFastPopAttempts();
                SpinLockPause();
                continue;
            }

            if constexpr (TMPMCRingQueueStats::CollectStatistics) {
                if (slot.Generation <= generation) {
                    TMPMCRingQueueStats::IncrementInvalidatedSlotsInReallyFastPop();
                }
            }

            if (slot.Generation > generation) {
                TMPMCRingQueueStats::IncrementFailedReallyFastPopAttempts();
                SpinLockPause();
                continue;
            }

            ui64 currentTail = Tail.load(std::memory_order_acquire);
            while (currentTail <= currentHead) {
                if (Tail.compare_exchange_weak(currentTail, currentHead + 1)) {
                    TMPMCRingQueueStats::IncrementFailedReallyFastPops();
                    return std::nullopt;
                }
            }

            SpinLockPause();
        }
    }
};

}  // NActors