#pragma once

#include <library/cpp/threading/chunk_queue/queue.h>
#include <util/system/mutex.h>

#include <atomic>
#include <optional>
#include <unordered_set>


namespace NActors {
    

struct TMPMCBitMapBuffer {

    const ui64 MaxSize;
    const ui64 SizeBits;
    NThreading::TPadded<TArrayHolder<std::atomic<ui64>>> Buffer;
    NThreading::TPadded<TArrayHolder<NThreading::TPadded<std::atomic<ui64>>>> Bitmap;
    NThreading::TPadded<std::atomic<ui64>> TailSize = 0;

    struct TTailSize {
        ui32 Tail = 0;
        ui32 Size = 0;

        static TTailSize Recognise(ui64 tailSize, ui64 sizeBits) {
            return {
                .Tail = static_cast<ui32>((tailSize >> sizeBits + 1) & ((1ull << sizeBits) - 1)),
                .Size = static_cast<ui32>(tailSize & ((1ull << sizeBits + 1) - 1)),
            };
        }

        ui64 Compact(ui64 sizeBits) const {
            return (Tail << sizeBits + 1) + Size;
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


    TMPMCBitMapBuffer(ui64 sizeBits)
        : MaxSize(1ull << sizeBits) 
        , SizeBits(sizeBits)
        , Buffer(new std::atomic<ui64>[MaxSize])
        , Bitmap(new NThreading::TPadded<std::atomic<ui64>>[MaxSize / 64])
    {
        for (ui32 idx = 0; idx < MaxSize; ++idx) {
            if (idx % 64 == 0) {
                Bitmap[idx / 64].store(0);
            }
            Buffer[idx] = TSlot::MakeEmpty(0);
        }
    }
    
    ui32 ConvertIdx(ui32 idx) {
        idx = idx % MaxSize;
        if (MaxSize < 0x100) {
            return idx;
        }
        // 0, 16, 32, .., 240,
        // 1, 17, 33, .., 241,
        // ...
        // 15, 31, 63, ..., 255,
        return (idx & ~0xff) | ((idx & 0xf) << 4) | ((idx >> 4) & 0xf);
    }

    std::optional<TTailSize> IncrementTailSize() {
        ui64 tailSize = TailSize.load(std::memory_order_acquire);
        TTailSize hts;
        do {
            hts = TTailSize::Recognise(tailSize, SizeBits);
            if (hts.Size == MaxSize) {
                return std::nullopt;
            }
            hts.Tail++;
            if (hts.Tail == MaxSize) {
                hts.Tail = 0;
            }
            hts.Size++;
        } while (!TailSize.compare_exchange_strong(tailSize, hts.Compact(SizeBits), std::memory_order_acq_rel));
        return hts;
    }

    std::optional<TTailSize> IncrementTail() {
        ui64 tailSize = TailSize.load(std::memory_order_acquire);
        TTailSize hts;
        do {
            hts = TTailSize::Recognise(tailSize, SizeBits);
            if (hts.Size == MaxSize) {
                return std::nullopt;
            }
            hts.Tail++;
            if (hts.Tail == MaxSize) {
                hts.Tail = 0;
            }
        } while (!TailSize.compare_exchange_strong(tailSize, hts.Compact(SizeBits), std::memory_order_acq_rel));
        // // Cerr << "New tail: " << hts.Tail << Endl;
        return hts;
    }

    TTailSize DecrementSize() {
        ui64 tailSize = TailSize.load(std::memory_order_acquire);
        TTailSize hts;
        do {
            hts = TTailSize::Recognise(tailSize, SizeBits);
            hts.Size--;
        } while (!TailSize.compare_exchange_strong(tailSize, hts.Compact(SizeBits), std::memory_order_acq_rel));
        return hts;
    }

    bool Push(ui32 el) {
        // // Cerr << "Push element: " << el << "\n";
        std::optional<TTailSize> hts = IncrementTailSize();
        if (!hts) {
            return false;
        }

        for (;;) {
            ui64 pos = hts->Tail - 1;
            // // Cerr << "Check position: " << pos << Endl;
            if (!CheckBit(pos)) {
                ui64 generation = pos / MaxSize;
                ui64 expected = TSlot::MakeEmpty(generation);
                TSlot parsedSlot = TSlot::Recognise(expected);
                std::atomic<ui64> &slot = Buffer[ConvertIdx(pos)];
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
            if (!hts) {
                return false;
            }
        }

        SetBit(hts->Tail - 1);
        return true;
    }

    std::string ConvertToBits(ui64 mask) {
        std::string  result;
        for (ui64 idx = 0; idx < 16; ++idx) {
            ui64 digit = mask & 0xf;
            mask >>= 4;
            if (digit < 10) {
                result += '0' + digit;
            } else {
                result += 'a' + digit - 10;
            }
        }
        std::reverse(result.begin(), result.end());
        return result;
    }

    std::optional<ui64> GetNearNotEmpty(ui64 pos) {
        // // Cerr << "GetNearNotEmpty" << Endl;
        ui64 generation = pos / MaxSize;
        ui64 idx = pos % MaxSize;
        ui64 startMaskIdx = idx / 64;
        ui64 maskIdx = startMaskIdx;
        ui64 startBitIdx = idx % 64;
        ui64 bitIdx = startBitIdx;
        // // Cerr << "startPosition: " << pos << " generation: " << generation << " maskIdx: " << maskIdx << " bitIdx: " << bitIdx << Endl;
        do {
            auto mask = Bitmap[maskIdx].load(std::memory_order_acquire);
            // // Cerr << "maskIdx: " << maskIdx << " bitIdx: " << bitIdx << " mask: " << ConvertToBits(mask) << Endl;
            if (mask) {
                if (bitIdx) {
                    mask &= ~((1ull << bitIdx) - 1);
                    // // Cerr << "mask update: " << ConvertToBits(((1ull << bitIdx) - 1)) << " mask: " << ConvertToBits(mask) << Endl;
                }
                if (mask) {
                    bitIdx = CountTrailingZeroBits(mask);
                    ui64 nextPosition = generation * MaxSize + maskIdx * 64 + bitIdx;
                    // // Cerr << "nextPosition: " << nextPosition << " generation: " << generation << " maskIdx: " << maskIdx << " bitIdx: " << bitIdx << " mask: " << ConvertToBits(mask) << Endl;
                    return nextPosition;
                }
            }
            bitIdx = 0;
            if (++maskIdx >= (MaxSize + 63) / 64) {
                generation++;
                maskIdx = 0;
            }
        } while (maskIdx != startMaskIdx);
        auto mask = Bitmap[maskIdx].load(std::memory_order_acquire);
        // // Cerr << "last maskIdx: " << maskIdx << " bitIdx: " << bitIdx << " mask: " << ConvertToBits(mask) << Endl;
        if (mask) {
            if (startBitIdx) {
                mask &= ((1ull << startBitIdx) - 1);
                // // Cerr << "mask update: " << ConvertToBits(((1ull << startBitIdx) - 1)) << " mask: " << ConvertToBits(mask) << Endl;
            }
            if (mask) {
                bitIdx = CountTrailingZeroBits(mask);
                ui64 nextPosition = generation * MaxSize + maskIdx * 64 + bitIdx;
                // // Cerr << "nextPosition: " << nextPosition << " generation: " << generation << " maskIdx: " << maskIdx << " bitIdx: " << bitIdx << " mask: " << ConvertToBits(mask) << Endl;
                return nextPosition;
            }
        }
        return std::nullopt;
    }

    template <typename TPred>
    std::optional<ui32> Find(ui64 pos, TPred pred) {
        // // Cerr << "Find\n";
        ui64 tailSize = TailSize.load(std::memory_order_acquire);
        TTailSize hts = TTailSize::Recognise(tailSize, SizeBits); 
        if (!hts.Size) {
            // // Cerr << "Didn't have elements" << Endl;
            return std::nullopt;
        }

        ui64 stopPosition = pos + MaxSize;

        // // Cerr << "Start find from begin: " << pos << " to end: " << stopPosition << "\n";
        auto nextPos = GetNearNotEmpty(pos);
        if (!nextPos) {
            // // Cerr << "Can't find first position" << Endl;
            return std::nullopt;
        }
        pos = *nextPos;
        while (pos < stopPosition) {
            std::atomic<ui64> &slot = Buffer[ConvertIdx(pos)];
            ui64 expected = slot.load(std::memory_order_acquire);
            TSlot parsedSlot = TSlot::Recognise(expected);
            if (!parsedSlot.IsEmpty) {
                // // Cerr << "Check element: " << parsedSlot.Value << Endl;
                if (pred(parsedSlot.Value)) {
                    slot.store(TSlot::MakeEmpty(0), std::memory_order_release);
                    UnsetBit(pos);
                    DecrementSize();
                    return parsedSlot.Value;
                }
            }
            nextPos = GetNearNotEmpty(pos + 1);
            if (!nextPos) {
                // // Cerr << "Didn't find element" << Endl;
                return std::nullopt;
            }
            pos = *nextPos;
        }
        // // Cerr << "Didn't find suitable element" << Endl;
        return std::nullopt;
    }

    bool CheckBit(ui64 idx) {
        idx %= MaxSize;
        ui64 maskIdx = idx / 64;
        ui64 bitIdx = idx % 64;
        std::atomic<ui64> &mask = Bitmap[maskIdx];
        ui64 currentMask = mask.load(std::memory_order_acquire);
        return currentMask & (1ull << bitIdx);
    }

    void SetBit(ui64 idx) {
        // // Cerr << "SetBit\n";
        idx %= MaxSize;
        ui64 maskIdx = idx / 64;
        ui64 bitIdx = idx % 64;
        // // Cerr << "Set bit at maskIdx: " << maskIdx << " bitIdx: " << bitIdx << Endl;
        std::atomic<ui64> &mask = Bitmap[maskIdx];
        ui64 currentMask = mask.load(std::memory_order_acquire);
        for (;;) {
            if (currentMask & (1ull << bitIdx)) {
                break;
            }
            if (mask.compare_exchange_strong(currentMask, currentMask | (1ull << bitIdx), std::memory_order_acq_rel)){
                break;
            }
            SpinLockPause();
        }
        // // Cerr << "Set bit at maskIdx: " << maskIdx << " bitIdx: " << bitIdx << " bit: " << ConvertToBits((1ull << bitIdx)) << " mask: " << ConvertToBits(currentMask | (1ull << bitIdx)) << Endl;
    }

    void UnsetBit(ui64 idx) {
        // // Cerr << "UnsetBit\n";
        idx %= MaxSize;
        ui64 maskIdx = idx / 64;
        ui64 bitIdx = idx % 64;
        // // Cerr << "Unset bit at maskIdx: " << maskIdx << " bitIdx: " << bitIdx << Endl;
        std::atomic<ui64> &mask = Bitmap[maskIdx];
        ui64 currentMask = mask.load(std::memory_order_acquire);
        for (;;) {
            if (!(currentMask & (1ull << bitIdx))) {
                break;
            }
            if (mask.compare_exchange_strong(currentMask, currentMask & ~(1ull << bitIdx), std::memory_order_acq_rel)){
                break;
            }
            SpinLockPause();
        }
        // // Cerr << "Unset bit at maskIdx: " << maskIdx << " bitIdx: " << bitIdx << " bit: " << ConvertToBits((1ull << bitIdx)) << " mask: " << ConvertToBits(currentMask & ~(1ull << bitIdx)) << Endl;
    }

};

}