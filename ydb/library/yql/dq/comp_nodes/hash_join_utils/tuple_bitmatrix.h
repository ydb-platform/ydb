#pragma once

#include <util/system/compiler.h>
#include <util/system/types.h>

namespace NKikimr::NMiniKQL::NPackedTuple {
namespace {

// Transpose 8x8 bit-matrix packed in ui64 integer
Y_FORCE_INLINE ui64 transposeBitmatrix(ui64 x) {
    /// fast path
    if (x == ~0ull) {
        return x;
    }

    /// transpose 1x1 diagonal elements in 2x2 block
    x = ((x &
          0b10101010'01010101'10101010'01010101'10101010'01010101'10101010'01010101ull)) |
        ((x &
          0b01010101'00000000'01010101'00000000'01010101'00000000'01010101'00000000ull) >>
         7) |
        ((x &
          0b00000000'10101010'00000000'10101010'00000000'10101010'00000000'10101010ull)
         << 7);

    /// transpose 2x2 diagonal elements in 4x4 block
    x = ((x &
          0b1100110011001100'0011001100110011'1100110011001100'0011001100110011ull)) |
        ((x &
          0b0011001100110011'0000000000000000'0011001100110011'0000000000000000ull) >>
         14) |
        ((x &
          0b0000000000000000'1100110011001100'0000000000000000'1100110011001100ull)
         << 14);

    /// transpose 4x4 diagonal elements in 8x8 block
    x = ((x &
          0b11110000111100001111000011110000'00001111000011110000111100001111ull)) |
        ((x &
          0b00001111000011110000111100001111'00000000000000000000000000000000ull) >>
         28) |
        ((x &
          0b00000000000000000000000000000000'11110000111100001111000011110000ull)
         << 28);

    return x;
}

[[maybe_unused]] void transposeBitmatrix(ui8 dst[], const ui8 *src[], const size_t row_size) {
    ui64 x = 0;
    for (size_t ind = 0; ind != 8; ++ind) {
        x |= ui64(*src[ind]) << (ind * 8);
    }

    x = transposeBitmatrix(x);

    for (size_t ind = 0; ind != 8; ++ind) {
        dst[ind * row_size] = x;
        x >>= 8;
    }
}

[[maybe_unused]] void transposeBitmatrix(ui8 *dst[], const ui8 src[], const size_t row_size) {
    ui64 x = 0;
    for (size_t ind = 0; ind != 8; ++ind) {
        x |= ui64(src[ind * row_size]) << (ind * 8);
    }

    x = transposeBitmatrix(x);

    for (size_t ind = 0; ind != 8; ++ind) {
        *dst[ind] = x;
        x >>= 8;
    }
}

} // namespace
} // namespace NKikimr::NMiniKQL::NPackedTuple
