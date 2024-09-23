#pragma once
#include "defs.h"

#include <library/cpp/threading/chunk_queue/queue.h>

#include <atomic>
#include <optional>

namespace NActors {

template <ui32 MaxSizeBits, ui32 ThreadBits=8>
struct TMPMCBlockingRingQueue {
    static constexpr std::optional<ui32> TestSize = std::nullopt;
    static constexpr ui32 MaxSize = 1 << MaxSizeBits;
    static constexpr ui32 MaxThreadCount = 1 << ThreadBits;
    
    struct TClaimedSlot {
        ui64 Slot;
        ui64 Generation;
        ui64 Head;
        std::optional<ui32> Value = std::nullopt;
        bool ByClaim = false;
    };

    struct alignas(ui64) TSlot {
        static constexpr ui64 EmptyBit = 1ull << 63;
        static constexpr ui64 ClaimBit = 1ull << 62;
        static constexpr ui64 InternalBits = EmptyBit | ClaimBit;
        ui64 Generation = 0;
        ui64 Value = 0;
        bool IsEmpty = true;
        bool IsClaim = false;

        static constexpr ui64 MakeEmpty(ui64 generation) {
            return EmptyBit | generation;
        }

        static constexpr ui64 MakeClaim(ui64 generation) {
            return EmptyBit | ClaimBit | generation;
        }

        static constexpr TSlot Recognise(ui64 slotValue) {
            if (slotValue & ClaimBit) {
                return {.Generation = (~InternalBits & slotValue), .IsEmpty=true, .IsClaim=true};
            }
            if (slotValue & EmptyBit) {
                return {.Generation = (~InternalBits & slotValue), .IsEmpty=true, .IsClaim=false};
            }
            return {.Value= (~InternalBits & slotValue), .IsEmpty=false, .IsClaim=false};
        }
    };

    NThreading::TPadded<std::atomic<ui64>> Tail{0};
    NThreading::TPadded<std::atomic<ui64>> Head{0};
    NThreading::TPadded<TArrayHolder<std::atomic<ui64>>> Buffer;
    static constexpr ui32 ConvertIdx(ui32 idx) {
        idx = idx % MaxSize;
        if constexpr (TestSize && *TestSize < 0x100) {
            return idx;
        }

        // 0, 16, 32, .., 240,
        // 1, 17, 33, .., 241,
        // ...
        // 15, 31, 63, ..., 255,
        // 16-32 cache lines
        return (idx & ~0xff) | ((idx & 0xf) << 4) | ((idx >> 4) & 0xf);
    }

    NThreading::TPadded<std::atomic_bool> StopFlag = false;

    TMPMCBlockingRingQueue()
        : Buffer(new std::atomic<ui64>[MaxSize])
    {
        for (ui32 idx = 0; idx < MaxSize; ++idx) {
            Buffer[idx] = TSlot::MakeEmpty(0);
        }
    }

    ~TMPMCBlockingRingQueue() {
    }

    bool TryPush(ui32 val) {
        for (;;) {
            ui64 currentTail = Tail.fetch_add(1, std::memory_order_relaxed);
            ui32 generation = currentTail / MaxSize;

            std::atomic<ui64> &currentSlot = Buffer[ConvertIdx(currentTail)];
            TSlot slot;
            ui64 expected = TSlot::MakeEmpty(generation);
            do {
                if (currentSlot.compare_exchange_strong(expected, val)) {
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
        }
    }

    void TryMoveTail(ui64 currentHead, ui64 currentTail) {
        while (currentTail <= currentHead) {
            if (Tail.compare_exchange_weak(currentTail, currentHead + 1)) {
                return;
            }
        }
    }

    TClaimedSlot ClaimSlot() {
        for (;;) {
            ui64 currentHead = Head.fetch_add(1, std::memory_order_relaxed);
            ui32 idx = ConvertIdx(currentHead);
            ui32 generation = currentHead / MaxSize;
            auto &currentSlot = Buffer[idx];
            ui64 expected = currentSlot.load(std::memory_order_relaxed);
            TSlot expectedSlot = TSlot::Recognise(expected);
            if (!expectedSlot.IsEmpty) {
                if (currentSlot.compare_exchange_strong(expected, TSlot::MakeEmpty(generation + 1))) {
                    return {idx, generation, currentHead, expectedSlot.Value, false};
                }
                SpinLockPause();
                continue;
            }

            if (expectedSlot.Generation > generation) {
                SpinLockPause();
                continue;
            }

            if (currentSlot.compare_exchange_strong(expected, TSlot::MakeClaim(generation))) {
                return {idx, generation, currentHead};
            }

            expectedSlot = TSlot::Recognise(expected);
            if (!expectedSlot.IsEmpty) {
                if (currentSlot.compare_exchange_strong(expected, TSlot::MakeEmpty(generation + 1))) {
                    return {idx, generation, currentHead, expectedSlot.Value, false};
                }
            }
            SpinLockPause();
        }
    }

    std::optional<ui32> CheckSlot(TClaimedSlot &claimedSlot) {
        auto &currentSlot = Buffer[claimedSlot.Slot];
        ui64 expected  = currentSlot.load(std::memory_order_acquire);
        auto slot = TSlot::Recognise(expected);
        if (slot.IsEmpty && slot.Generation <= claimedSlot.Generation) {
            return std::nullopt;
        } else if (slot.IsEmpty) {
            claimedSlot = ClaimSlot();
            if (claimedSlot.Value) {
                return claimedSlot.Value;
            }
            return std::nullopt;
        }
        if (currentSlot.compare_exchange_strong(expected, TSlot::MakeEmpty(claimedSlot.Generation + 1))) {
            return slot.Value;
        }
        return std::nullopt;
    }

    std::optional<ui32> ForbidSlot(TClaimedSlot claimedSlot) {
        ui64 expected = TSlot::MakeClaim(claimedSlot.Generation);
        auto &currentSlot = Buffer[claimedSlot.Slot];
        if (currentSlot.compare_exchange_strong(expected, TSlot::MakeEmpty(claimedSlot.Generation + 1))) {
            TryMoveTail(claimedSlot.Head, Tail.load(std::memory_order_acquire));
            return std::nullopt;
        }
        auto slot = TSlot::Recognise(expected);
        if (slot.IsEmpty) {
            return std::nullopt;
        }
        if (currentSlot.compare_exchange_strong(expected, TSlot::MakeEmpty(claimedSlot.Generation + 1))) {
            return slot.Value;
        }
        return std::nullopt;
    }

    std::optional<ui32> TryPop() {
        TClaimedSlot claimedSlot = ClaimSlot();
        if (claimedSlot.Value) {
            return claimedSlot.Value;
        }
        for (;;) {
            if (auto read = CheckSlot(claimedSlot)) {
                return read;
            }
            if (StopFlag.load(std::memory_order_acquire)) {
                break;
            }
            SpinLockPause();
        }
        return ForbidSlot(claimedSlot);
    }

    void Stop() {
        StopFlag.store(true, std::memory_order_release);
    }
};

} // NActors
