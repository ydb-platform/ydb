#pragma once

#include <util/generic/typetraits.h>

#include <limits.h>

//! Convert signed values to unsigned. Convenient for further varint encoding.
//! See https://developers.google.com/protocol-buffers/docs/encoding#types for details.

template <typename TSignedInt>
inline auto ZigZagEncode(TSignedInt n) -> std::make_unsigned_t<TSignedInt> {
    static_assert(std::is_signed<TSignedInt>::value && std::is_integral<TSignedInt>::value, "Expected signed integral type.");
    auto un = static_cast<std::make_unsigned_t<TSignedInt>>(n);
    return (un << 1) ^ (n >> (CHAR_BIT * sizeof(TSignedInt) - 1));
}

template <typename TUnsignedInt>
inline auto ZigZagDecode(TUnsignedInt n) -> std::make_signed_t<TUnsignedInt> {
    static_assert(std::is_unsigned<TUnsignedInt>::value, "Expected unsigned integral type.");
    return (n >> 1) ^ -static_cast<std::make_signed_t<TUnsignedInt>>(n & 1);
}
