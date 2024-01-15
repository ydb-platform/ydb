#pragma once
#include "defs.h"

#include <library/cpp/threading/chunk_queue/queue.h>

#include <atomic>
#include <optional>

namespace NActors {

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
    } ;

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
        for (;;) {
            ui64 currentTail = Tail.load(std::memory_order_acquire);
            ui32 generation = currentTail / MaxSize;

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentTail)];
            TSlot slot;
            ui64 expected = TSlot::MakeEmpty(generation);
            do {
                if (currentSlot.compare_exchange_weak(expected, val)) {
                    Tail.compare_exchange_strong(currentTail, currentTail + 1);
                    return true;
                }
                slot = TSlot::Recognise(expected);
            } while (slot.Generation <= generation && slot.IsEmpty);

            if (!slot.IsEmpty) {
                ui64 currentHead = Head.load(std::memory_order_acquire);
                if (currentHead + MaxSize <= currentTail + std::min<ui64>(1024, MaxSize - 1)) {
                    return false;
                }
            }

            Tail.compare_exchange_strong(currentTail, currentTail + 1);
            SpinLockPause();
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
                return true;
            }
            slot = TSlot::Recognise(expected);
        } while (slot.Generation <= generation && slot.IsEmpty);

        if (!slot.IsEmpty) {
            ui64 currentHead = Head.load(std::memory_order_acquire);
            if (currentHead + MaxSize <= currentTail + std::min<ui64>(1024, MaxSize - 1)) {
                return false;
            }
        }
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
                    return std::nullopt;
                }
                if (slot.Generation == LocalGeneration) {
                    if (currentSlot.compare_exchange_strong(expected, TSlot::MakeEmpty(LocalGeneration + 1))) {
                        ShiftLocalHead();
                    }
                }
                SpinLockPause();
                continue;
            }
            currentSlot.store(TSlot::MakeEmpty(LocalGeneration + 1), std::memory_order_release);
            ShiftLocalHead();
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
            }
        }
        if (currentHead == currentTail) {
            return std::nullopt;
        }

        return TryPopSlow(currentHead);
    }

    std::optional<ui32> TryPopSlow(ui64 currentHead = 0) {
        if (!currentHead) {
            currentHead = Head.load(std::memory_order_acquire);
        }
        for (ui32 it = 0; it < 3; ++it) {
            ui32 generation = currentHead / MaxSize;

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentHead)];

            ui64 expected = currentSlot.load(std::memory_order_relaxed);
            TSlot slot = TSlot::Recognise(expected);

            if (slot.Generation > generation) {
                Head.compare_exchange_strong(currentHead, currentHead + 1);
                SpinLockPause();
                continue;
            }

            while (generation > slot.Generation) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation))) {
                    if (!slot.IsEmpty) {
                        Head.compare_exchange_strong(currentHead, currentHead + 1);
                        return slot.Value;
                    }
                    break;
                }
                slot = TSlot::Recognise(expected);
            }

            while (!slot.IsEmpty) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    Head.compare_exchange_strong(currentHead, currentHead + 1);
                    return slot.Value;
                }
                slot = TSlot::Recognise(expected);
            }

            if (slot.Generation > generation) {
                Head.compare_exchange_strong(currentHead, currentHead + 1);
                SpinLockPause();
                continue;
            }

            ui64 currentTail = Tail.load(std::memory_order_acquire);
            if (currentTail <= currentHead) {
                return std::nullopt;
            }

            while (slot.Generation == generation && slot.IsEmpty) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    Head.compare_exchange_strong(currentHead, currentHead + 1);
                    break;
                }
                slot = TSlot::Recognise(expected);
            }

            SpinLockPause();
            currentHead = Head.load(std::memory_order_acquire);
        }
        return std::nullopt;
    }

    std::optional<ui32> TryPopFast() {
        for (;;) {
            ui64 currentHead = Head.fetch_add(1, std::memory_order_relaxed);
            ui32 generation = currentHead / MaxSize;

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentHead)];

            ui64 expected = currentSlot.load(std::memory_order_relaxed);
            TSlot slot = TSlot::Recognise(expected);

            if (slot.Generation > generation) {
                SpinLockPause();
                continue;
            }

            while (generation >= slot.Generation) {
                if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(generation + 1))) {
                    if (!slot.IsEmpty) {
                        return slot.Value;
                    }
                    break;
                }
                slot = TSlot::Recognise(expected);
            }

            if (slot.Generation > generation) {
                SpinLockPause();
                continue;
            }

            ui64 currentTail = Tail.load(std::memory_order_acquire);
            if (currentTail > currentHead) {
                SpinLockPause();
                continue;
            }

            while (currentTail <= currentHead) {
                if (Tail.compare_exchange_weak(currentTail, currentHead + 1)) {
                    return std::nullopt;
                }
            }
            return std::nullopt;
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
                return slot.Value;
            }

            if (slot.Generation > generation) {
                expected = TSlot::MakeEmpty(generation + 1);
                TSlot slot2 = TSlot::Recognise(expected);
                while (slot.Generation > slot2.Generation) {
                    if (currentSlot.compare_exchange_weak(expected, TSlot::MakeEmpty(slot.Generation))) {
                        if (!slot2.IsEmpty) {
                            return slot2.Value;
                        }
                        break;
                    }
                    slot2 = TSlot::Recognise(expected);
                }
                SpinLockPause();
                continue;
            }

            if (slot.Generation > generation) {
                SpinLockPause();
                continue;
            }

            ui64 currentTail = Tail.load(std::memory_order_acquire);
            while (currentTail < currentHead) {
                if (Tail.compare_exchange_weak(currentTail, currentHead + 1)) {
                    return std::nullopt;
                }
            }
            SpinLockPause();
        }
    }
};

}  // NActors