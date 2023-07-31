#pragma once

#include <util/system/types.h>

namespace NReverseGeocoder {
    constexpr ui64 B = 1ull;
    constexpr ui64 KB = 1024 * B;
    constexpr ui64 MB = 1024 * KB;
    constexpr ui64 GB = 1024 * MB;

    constexpr size_t MEMORY_ALIGNMENT = 16ull;

    inline unsigned long long AlignMemory(unsigned long long x) {
        if (x % MEMORY_ALIGNMENT == 0)
            return x;
        return x + MEMORY_ALIGNMENT - x % MEMORY_ALIGNMENT;
    }

    inline bool IsAlignedMemory(void* ptr) {
        return ((uintptr_t)ptr) % MEMORY_ALIGNMENT == 0;
    }

}
