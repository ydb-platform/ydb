#pragma once

#include "defs.h"
#include <yql/essentials/utils/swap_bytes.h>

#include <array>

namespace NYql::NUdf {

inline ui8* CompressAsSparseBitmap(const ui8* src, size_t srcOffset, const ui8* sparseBitmap, ui8* dst, size_t count) {
    while (count--) {
        ui8 inputBit = (src[srcOffset >> 3] >> (srcOffset & 7)) & 1;
        *dst = inputBit;
        ++srcOffset;
        dst += *sparseBitmap++;
    }
    return dst;
}

inline ui8* DecompressToSparseBitmap(ui8* dstSparse, const ui8* src, size_t srcOffset, size_t count) {
    if (srcOffset != 0) {
        size_t offsetBytes = srcOffset >> 3;
        size_t offsetTail = srcOffset & 7;
        src += offsetBytes;
        if (offsetTail != 0) {
            for (ui8 i = offsetTail; count > 0 && i < 8; i++, count--) {
                *dstSparse++ = (*src >> i) & 1u;
            }
            src++;
        }
    }
    while (count >= 8) {
        ui8 slot = *src++;
        *dstSparse++ = (slot >> 0) & 1u;
        *dstSparse++ = (slot >> 1) & 1u;
        *dstSparse++ = (slot >> 2) & 1u;
        *dstSparse++ = (slot >> 3) & 1u;
        *dstSparse++ = (slot >> 4) & 1u;
        *dstSparse++ = (slot >> 5) & 1u;
        *dstSparse++ = (slot >> 6) & 1u;
        *dstSparse++ = (slot >> 7) & 1u;
        count -= 8;
    }
    for (ui8 i = 0; i < count; i++) {
        *dstSparse++ = (*src >> i) & 1u;
    }
    return dstSparse;
}

template <bool Negate>
inline void CompressSparseImpl(ui8* dst, const ui8* srcSparse, size_t len) {
    while (len >= 8) {
        ui8 result = 0;
        result |= (*srcSparse++ & 1u) << 0;
        result |= (*srcSparse++ & 1u) << 1;
        result |= (*srcSparse++ & 1u) << 2;
        result |= (*srcSparse++ & 1u) << 3;
        result |= (*srcSparse++ & 1u) << 4;
        result |= (*srcSparse++ & 1u) << 5;
        result |= (*srcSparse++ & 1u) << 6;
        result |= (*srcSparse++ & 1u) << 7;
        if constexpr (Negate) {
            *dst++ = ~result;
        } else {
            *dst++ = result;
        }
        len -= 8;
    }
    if (len) {
        ui8 result = 0;
        for (ui8 i = 0; i < len; ++i) {
            result |= (*srcSparse++ & 1u) << i;
        }
        if constexpr (Negate) {
            *dst++ = ~result;
        } else {
            *dst++ = result;
        }
    }
}

inline void CompressSparseBitmap(ui8* dst, const ui8* srcSparse, size_t len) {
    return CompressSparseImpl<false>(dst, srcSparse, len);
}

inline void CompressSparseBitmapNegate(ui8* dst, const ui8* srcSparse, size_t len) {
    return CompressSparseImpl<true>(dst, srcSparse, len);
}

template <typename T>
inline T* CompressArray(const T* src, const ui8* sparseBitmap, T* dst, size_t count) {
    while (count--) {
        *dst = *src++;
        dst += *sparseBitmap++;
    }
    return dst;
}

inline void CopyDenseBitmap(ui8* dst, const ui8* src, size_t srcOffset, size_t len) {
    if ((srcOffset & 7) != 0) {
        size_t offsetBytes = srcOffset >> 3;
        src += offsetBytes;

        ui8 offsetTail = srcOffset & 7;
        ui8 offsetHead = 8 - offsetTail;

        ui8 remainder = *src++ >> offsetTail;
        size_t dstOffset = offsetHead;
        for (; dstOffset < len; dstOffset += 8) {
            *dst++ = remainder | (*src << offsetHead);
            remainder = *src >> offsetTail;
            src++;
        }
        // dst is guaranteed to have extra length even if it's not needed
        *dst++ = remainder;
    } else {
        src += srcOffset >> 3;
        // Round up to 8
        len = (len + 7u) & ~size_t(7u);
        memcpy(dst, src, len >> 3);
    }
}

// Duplicates every bit in an 8-bit value.
// Example: 0b01010101 -> 0b0011001100110011.
Y_FORCE_INLINE ui16 ReplicateEachBitTwice(ui8 b) {
    ui16 x = b;
    x = (x | (x << 4)) & 0x0F0F;
    x = (x | (x << 2)) & 0x3333;
    x = (x | (x << 1)) & 0x5555;
    return x | (x << 1);
}

// Repeat 4 times every bit in an 8-bit value.
// Example: 0b01010101 -> 0b00001111000011110000111100001111.
Y_FORCE_INLINE ui32 ReplicateEachBitFourTimes(ui8 b) {
    ui32 x = b;
    x = (x | (x << 12)) & 0x000F000F;
    x = (x | (x << 6)) & 0x03030303;
    x = (x | (x << 3)) & 0x11111111;
    x *= 0x0F;
    return x;
}

// Repeat 8 times every bit in an 8-bit value.
// Example: 0b01010101 -> 0b0000000011111111000000001111111100000000111111110000000011111111.
Y_FORCE_INLINE ui64 ReplicateEachBitEightTimes(ui8 x) {
    ui64 expanded = x;
    expanded = (expanded * 0x8040201008040201ULL);
    expanded &= 0x8080808080808080ULL;
    expanded >>= 7;
    expanded *= 0xFF;
    expanded = NYql::SwapBytes(expanded);
    return expanded;
}

// BitToByteExpand - Expands the individual bits of an 8-bit input x into an array of 8 elements of type TType.
// Each output element corresponds to one bit from the original value, expanded (via specialized routines) to fill the entire TType
// Example: BitToByteExpand<ui8>(0b10101010) yields REVERSE({0xFF, 0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF, 0x00}).
// Example: BitToByteExpand<ui16>(0b11000011) yields REVERSE({0xFFFF, 0xFFFF, 0x0000, 0x0000, 0x0000, 0x0000, 0xFFFF, 0xFFFF}).
template <typename TType>
Y_FORCE_INLINE std::array<TType, 8> BitToByteExpand(ui8 x);

template <>
Y_FORCE_INLINE std::array<ui8, 8> BitToByteExpand(ui8 x) {
    std::array<ui8, 8> result;
    ui64 expanded = ReplicateEachBitEightTimes(x);
    memcpy(&result[0], &expanded, sizeof(expanded));
    return result;
}

template <>
Y_FORCE_INLINE std::array<ui16, 8> BitToByteExpand(ui8 x) {
    std::array<ui8, 8> input = BitToByteExpand<ui8>(x);
    std::array<ui16, 8> output{};

    for (size_t i = 0; i < 8; ++i) {
        output[i] = ReplicateEachBitTwice(input[i]);
    }

    return output;
}

template <>
Y_FORCE_INLINE std::array<ui32, 8> BitToByteExpand(ui8 x) {
    std::array<ui8, 8> input = BitToByteExpand<ui8>(x);
    std::array<ui32, 8> output{};

    for (size_t i = 0; i < 8; ++i) {
        output[i] = ReplicateEachBitFourTimes(input[i]);
    }

    return output;
}

template <>
Y_FORCE_INLINE std::array<ui64, 8> BitToByteExpand(ui8 x) {
    std::array<ui8, 8> input = BitToByteExpand<ui8>(x);
    std::array<ui64, 8> output{};

    for (size_t i = 0; i < 8; ++i) {
        output[i] = ReplicateEachBitEightTimes(input[i]);
    }

    return output;
}

} // namespace NYql::NUdf
