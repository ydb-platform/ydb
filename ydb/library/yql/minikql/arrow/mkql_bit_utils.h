#pragma once
#include <util/system/types.h>
#include <ydb/library/yql/public/udf/arrow/bit_util.h>

namespace NKikimr {
namespace NMiniKQL {

inline ui64 SaturationSub(ui64 x, ui64 y) {
    // simple code produces cmov (same result as commented one)
    return (x > y) ? x - y : 0;
    //ui64 res = x - y;
    //res &= -(res <= x);
    //return res;
}

inline ui8 LoadByteUnaligned(const ui8* bitmap, size_t bitmapOffset) {
    size_t byteOffset = bitmapOffset >> 3;
    ui8 bit = ui8(bitmapOffset & 7u);

    ui8 first = bitmap[byteOffset];
    // extend to ui32 to avoid left UB in case of left shift of byte by 8 bit
    ui32 second = bitmap[byteOffset + (bit != 0)];

    return (first >> bit) | ui8(second << (8 - bit));
}

template<typename T>
concept BitwiseOperable = requires(T a, T b) {
    { a & b };
    { a | b };
    { -a };
};

template<typename T>
inline T SelectArg(ui8 isFirst, T first, T second) {
    if constexpr (std::is_floating_point<T>::value) {
        return isFirst ? first : second;
    } else if constexpr(BitwiseOperable<T>) {
        // isFirst == 1 -> mask 0xFF..FF, isFirst == 0 -> mask 0x00..00
        T mask = -T(isFirst);
        return (first & mask) | (second & ~mask);
    } else {
        static_assert(BitwiseOperable<T>, "Type must support bitwise operations");
    }
}

inline ui8 CompressByte(ui8 x, ui8 m) {
    // algorithm 7-4 (Compress or Generalized Extract) from second edition of "Hacker's Delight" (single byte version)
    // compresses bits from x according to mask m
    // X:                01101011
    // M:                00110010
    // MASKED VALUES:    --10--1-
    // RESULT:           00000101
    // TODO: should be replaced by PEXT instruction from BMI2 instruction set
    ui8 mk, mp, mv, t;
    x = x & m;    // Clear irrelevant bits.
    mk = ~m << 1; // We will count 0's to right.
    for (ui8 i = 0; i < 3; i++) {
        mp = mk ^ (mk << 1);    // Parallel suffix.
        mp = mp ^ (mp << 2);
        mp = mp ^ (mp << 4);
        mv = mp & m;     // Bits to move.
        m = m ^ mv | (mv >> (1 << i)); // Compress m.
        t = x & mv;
        x = x ^ t | (t >> (1 << i));   // Compress x.
        mk = mk & ~mp;
    }
    return x;
}

inline ui8 PopCountByte(ui8 value) {
    static constexpr uint8_t bytePopCounts[] = {
        0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3,
        4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4,
        4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4,
        5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5,
        4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2,
        3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5,
        5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4,
        5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6,
        4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};
    return bytePopCounts[value];
}

inline size_t GetSparseBitmapPopCount(const ui8* src, size_t len) {
    // auto-vectorization is performed here
    size_t result = 0;
    while (len--) {
        result += *src++;
    }
    return result;
}

using NYql::NUdf::CompressSparseBitmap;
using NYql::NUdf::CompressSparseBitmapNegate;

inline void NegateSparseBitmap(ui8* dst, const ui8* src, size_t len) {
    while (len--) {
        *dst++ = *src++ ^ ui8(1);
    }
}

inline void XorSparseBitmapScalar(ui8* dst, ui8 value, const ui8* src, size_t len) {
    while (len--) {
        *dst++ = *src++ ^ value;
    }
}

inline void XorSparseBitmaps(ui8* dst, const ui8* src1, const ui8* src2, size_t len) {
    while (len--) {
        *dst++ = *src1++ ^ *src2++;
    }
}

inline void AndSparseBitmaps(ui8* dst, const ui8* src1, const ui8* src2, size_t len) {
    while (len--) {
        *dst++ = *src1++ & *src2++;
    }
}

inline void OrSparseBitmaps(ui8* dst, const ui8* src1, const ui8* src2, size_t len) {
    while (len--) {
        *dst++ = *src1++ | *src2++;
    }
}

inline size_t CompressBitmap(const ui8* src, size_t srcOffset,
    const ui8* bitmap, size_t bitmapOffset, ui8* dst, size_t dstOffset, size_t count)
{
    //  TODO: 1) aligned version (srcOffset % 8 == 0), (srcOffset % 8 == 0)
    //        2) 64 bit processing (instead of 8)
    ui8* target = dst + (dstOffset >> 3);
    ui8 state = *target;
    ui8 stateBits = dstOffset & 7u;
    state &= (ui32(1) << stateBits) - 1u;
    while (count) {
        ui8 srcByte = LoadByteUnaligned(src, srcOffset);
        ui8 bitmapByte = LoadByteUnaligned(bitmap, bitmapOffset);

        // zero all bits outside of input range
        bitmapByte &= ui8(0xff) >> ui8(SaturationSub(8u, count));

        ui8 compressed = CompressByte(srcByte, bitmapByte);
        ui8 compressedBits = PopCountByte(bitmapByte);

        state |= (compressed << stateBits);
        *target = state;
        stateBits += compressedBits;
        target += (stateBits >> 3);

        ui8 overflowState = ui32(compressed) >> (compressedBits - SaturationSub(stateBits, 8));

        ui8 mask = (stateBits >> 3) - 1;
        state = (state & mask) | (overflowState & ~mask);

        stateBits &= 7;
        dstOffset += compressedBits;
        srcOffset += 8;
        bitmapOffset += 8;
        count = SaturationSub(count, 8u);
    }
    *target = state;
    return dstOffset;
}

using NYql::NUdf::CompressAsSparseBitmap;
using NYql::NUdf::CompressArray;
using NYql::NUdf::DecompressToSparseBitmap;

} // namespace NMiniKQL
} // namespace NKikimr
