/*******************************************************************************
 * tlx/math/popcount.hpp
 *
 * popcount() population count number of one bits - mainly for portability.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_MATH_POPCOUNT_HEADER
#define TLX_MATH_POPCOUNT_HEADER

#include <cstdint>
#include <cstdlib>

#ifdef _MSC_VER
#include <intrin.h>
#endif

namespace tlx {

//! \addtogroup tlx_math
//! \{

/******************************************************************************/
// popcount() - count one bits

//! popcount (count one bits) - generic SWAR implementation
static inline unsigned popcount_generic8(std::uint8_t x) {
    x = x - ((x >> 1) & 0x55);
    x = (x & 0x33) + ((x >> 2) & 0x33);
    return static_cast<std::uint8_t>((x + (x >> 4)) & 0x0F);
}

//! popcount (count one bits) - generic SWAR implementation
static inline unsigned popcount_generic16(std::uint16_t x) {
    x = x - ((x >> 1) & 0x5555);
    x = (x & 0x3333) + ((x >> 2) & 0x3333);
    return static_cast<std::uint16_t>(((x + (x >> 4)) & 0x0F0F) * 0x0101) >> 8;
}

//! popcount (count one bits) -
//! generic SWAR implementation from https://stackoverflow.com/questions/109023
static inline unsigned popcount_generic32(std::uint32_t x) {
    x = x - ((x >> 1) & 0x55555555);
    x = (x & 0x33333333) + ((x >> 2) & 0x33333333);
    return (((x + (x >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24;
}

//! popcount (count one bits) - generic SWAR implementation
static inline unsigned popcount_generic64(std::uint64_t x) {
    x = x - ((x >> 1) & 0x5555555555555555);
    x = (x & 0x3333333333333333) + ((x >> 2) & 0x3333333333333333);
    return (((x + (x >> 4)) & 0x0F0F0F0F0F0F0F0F) * 0x0101010101010101) >> 56;
}

/******************************************************************************/

#if defined(__GNUC__) || defined(__clang__)

//! popcount (count one bits)
static inline unsigned popcount(unsigned i) {
    return static_cast<unsigned>(__builtin_popcount(i));
}

//! popcount (count one bits)
static inline unsigned popcount(int i) {
    return popcount(static_cast<unsigned>(i));
}

//! popcount (count one bits)
static inline unsigned popcount(unsigned long i) {
    return static_cast<unsigned>(__builtin_popcountl(i));
}

//! popcount (count one bits)
static inline unsigned popcount(long i) {
    return popcount(static_cast<unsigned long>(i));
}

//! popcount (count one bits)
static inline unsigned popcount(unsigned long long i) {
    return static_cast<unsigned>(__builtin_popcountll(i));
}

//! popcount (count one bits)
static inline unsigned popcount(long long i) {
    return popcount(static_cast<unsigned long long>(i));
}

#elif defined(_MSC_VER)

//! popcount (count one bits)
template <typename Integral>
inline unsigned popcount(Integral i) {
    if (sizeof(i) <= sizeof(int))
        return __popcnt(i);
    else {
#if defined(_WIN64)
        return __popcnt64(i);
#else
        return popcount_generic64(i);
#endif
    }
}

#else

//! popcount (count one bits)
template <typename Integral>
inline unsigned popcount(Integral i) {
    if (sizeof(i) <= sizeof(std::uint8_t))
        return popcount_generic8(i);
    else if (sizeof(i) <= sizeof(std::uint16_t))
        return popcount_generic16(i);
    else if (sizeof(i) <= sizeof(std::uint32_t))
        return popcount_generic32(i);
    else if (sizeof(i) <= sizeof(std::uint64_t))
        return popcount_generic64(i);
    else
        abort();
}

#endif

/******************************************************************************/
// popcount range

static inline
size_t popcount(const void* data, size_t size) {
    const std::uint8_t* begin = reinterpret_cast<const std::uint8_t*>(data);
    const std::uint8_t* end = begin + size;
    size_t total = 0;
    while (begin + 7 < end) {
        total += popcount(*reinterpret_cast<const std::uint64_t*>(begin));
        begin += 8;
    }
    if (begin + 3 < end) {
        total += popcount(*reinterpret_cast<const std::uint32_t*>(begin));
        begin += 4;
    }
    while (begin < end) {
        total += popcount(*begin++);
    }
    return total;
}

//! \}

} // namespace tlx

#endif // !TLX_MATH_POPCOUNT_HEADER

/******************************************************************************/
