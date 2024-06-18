#pragma once

#include <library/cpp/threading/chunk_queue/queue.h>
#include <util/system/mutex.h>

#include <atomic>
#include <optional>


namespace NActors {
    

template <ui32 SIZE_BITS>
struct TMPMCBitMapBuffer {
    static_assert(SIZE_BITS * 2 <= 64);

    static constexpr ui64 MaxSize = 1ull << SIZE_BITS;
    NThreading::TPadded<TArrayHolder<std::atomic_uint64_t>> Buffer;
    NThreading::TPadded<TArrayHolder<NThreading::TPadded<std::atomic_uint64_t>>> Bitmap;
    NThreading::TPadded<std::atomic_uint64_t> TailSize = 0;

    struct TTailSize {
        ui32 Tail = 0;
        ui32 Size = 0;

        static TTailSize Recognise(ui64 tailSize) {
            return {
                .Tail = (tailSize >> SIZE_BITS) & ((1ull << SIZE_BITS) - 1); 
                .Size = tailSize & ((1ull << SIZE_BITS) - 1);
            };
        }

        operator ui64 () const {
            return (Tail << SIZE_BITS) + Size;
        }
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


    TMPMCBitMapBuffer()
        : Buffer(new std::atomic_uint64_t[MaxSize])
        , Bitmap(new NThreading::TPadded<std::atomic_uint64_t>[MaxSize / 64])
    {
        for (ui32 idx = 0; idx < MaxSize; ++idx) {
            if (idx % 64 == 0) {
                Bitmap[idx / 64] = 0;
            }
            Buffer[idx] = 0;
        }
    }
    
    static constexpr ui32 ConvertIdx(ui32 idx) {
        idx = idx % Size;
        if constexpr (MaxSize < 0x100) {
            return idx;
        }
        // 0, 16, 32, .., 240,
        // 1, 17, 33, .., 241,
        // ...
        // 15, 31, 63, ..., 255,
        return (idx & ~0xff) | ((idx & 0xf) << 4) | ((idx >> 4) & 0xf);
    }

    TTailSize IncrementTailSize() {
        ui64 tailSize = TailSize.load(std::memory_order_acquire);
        TTailSize hts;
        do {
            hts = TTailSize::Recognise(tailSize);
            hts.Tail++;
            hts.Size++;
        } while (!hts.compare_exchange_strong(tailSize, static_cast<ui64>(hts), std::memory_order_acq_rel));
        return hts;
    }

    TTailSize IncrementTail() {
        ui64 tailSize = TailSize.load(std::memory_order_acquire);
        TTailSize hts;
        do {
            hts = TTailSize::Recognise(tailSize);
            hts.Tail++;
        } while (!hts.compare_exchange_strong(tailSize, static_cast<ui64>(hts), std::memory_order_acq_rel));
        return hts;
    }

    void Push(ui32 el) {
        TTailSize hts = IncrementTailSize();

        for (;;) {
            ui64 pos = hts.Tail - 1;
            if (!CheckBit(pos)) {
                ui64 generation = pos / MaxSize;
                ui64 expected = TSlot::MakeEmpty(generation);
                TSlot parsedSlot = TSlot::Recognise(expected);
                std::atomic_uint64_t &slot = Buffer[ConvertIdx(pos)];
                bool success = false;
                do {
                    if (slot.compare_exchange_strong(expected, el, std::memory_order_acq_rel)) {
                        success = true;
                        break;
                    }
                    parsedSlot = TSlot::Recognise(expected);
                    SpinLockPause();
                } while (parsedSlot.IsEmpty && parsedSlot.Generation <= generation);
                if (success) {
                    break;
                }
            }
            hts = IncrementTail();
        }

        SetBit(hts.Tail - 1);
    }

    std::optional<ui64> GetNearNotEmpty(ui64 pos) {
        ui64 generation = pos / MaxSize;
        ui64 idx = pos % MaxSize;
        ui64 startMaskIdx = idx / 64;
        ui64 maskIdx = startMaskIdx;
        ui64 bitIdx = idx % 64;
        do {
            if (maskIdx == MaxSize / 64) {
                generation++;
                maskIdx = 0;
            }
            auto mask = Bitmap[maskIdx].load(std::memory_order_acquire);
            if (mask) {
                if (bitIdx) {
                    mask &= ~((1 << bitIdx) - 1);
                }
                if (mask) {
                    bitIdx = CountTrailingZeroBits(mask);
                    return generation * MaxSize + maskIdx * 64 + bitIdx;
                }
            }
            bitIdx = 0;
            if (++maskIdx == MaxSize / 64) {
                generation++;
                maskIdx = 0;
            }
        } while (maskIdx != startMaskIdx);
        return std::nullopt;
    }

    template <typename TPred>
    std::optional<ui32> Find(ui64 pos, TPred pred) {
        ui64 tailSize = TailSize.load(std::memory_order_acquire);
        TTailSize hts = TTailSize::Recognise(tailSize); 
        if (!hts.Size) {
            return std::nullopt;
        }

        ui64 stopPosition = pos + MaxSize;
        auto nextPos = GetNearNotEmpty(pos);
        if (!nextPos) {
            return std::nullopt;
        }
        pos = *nextPos;
        while (pos < stopPosition) {
            std::atomic_uint64_t &slot = Buffer[ConvertIdx(pos)];
            ui64 expected = slot.load(std::memory_order_acquire);
            TSlot parsedSlot = TSlot::Recognise(expected);
            if (!parsedSlot.IsEmpty) {
                if (pred(parsedSlot.Value)) {
                    slot.store(TSlot::MakeEmpty(0), std::memory_order_release);
                    UnsetBit(pos);
                    return parsedSlot.Value;
                }
            }
            nextPos = GetNearNotEmpty(pos + 1);
            if (!nextPos) {
                return std::nullopt;
            }
            pos = *nextPos;
        }
        return std::nullopt;
    }

    bool CheckBit(ui64 idx) {
        idx %= MaxSize;
        ui64 maskIdx = idx / 64;
        ui64 bitIdx = idx % 64;
        std::atomic_uint64_t &mask = Bitmap[maskIdx];
        ui64 currentMask = mask.load(std::memory_order_acquire);
        return currentMask & (1 << bitIdx);
    }

    void SetBit(ui64 idx) {
        idx %= MaxSize;
        ui64 maskIdx = idx / 64;
        ui64 bitIdx = idx % 64;
        std::atomic_uint64_t &mask = Bitmap[maskIdx];
        ui64 currentMask = mask.load(std::memory_order_acquire);
        for (;;) {
            if (currentMask & (1 << bitIdx)) {
                break;
            }
            if (mask.compare_exchange_strong(currentMask, currentMask | (1 << bitIdx), std::memory_order_acq_rel)){
                break;
            }
            SpinLockPause();
        }
    }

    void UnsetBit(ui64 idx) {
        idx %= MaxSize;
        ui64 maskIdx = idx / 64;
        ui64 bitIdx = idx % 64;
        std::atomic_uint64_t &mask = Bitmap[maskIdx];
        ui64 currentMask = mask.load(std::memory_order_acquire);
        for (;;) {
            if (!(currentMask & (1 << bitIdx))) {
                break;
            }
            if (mask.compare_exchange_strong(currentMask, currentMask & ~(1 << bitIdx), std::memory_order_acq_rel)){
                break;
            }
            SpinLockPause();
        }
    }

};

}