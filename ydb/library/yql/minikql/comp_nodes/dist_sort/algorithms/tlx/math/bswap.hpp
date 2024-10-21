/*******************************************************************************
 * tlx/math/bswap.hpp
 *
 * bswap16(), bswap32(), and bswap64() to swap bytes - mainly for portability.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_MATH_BSWAP_HEADER
#define TLX_MATH_BSWAP_HEADER

#include <cstdint>

#ifdef _MSC_VER
#include <cstdlib>
#endif

namespace tlx {

//! \addtogroup tlx_math
//! \{

/******************************************************************************/
// bswap16() - swap 16-bit integers

//! bswap16 - generic implementation
static inline std::uint16_t bswap16_generic(const std::uint16_t& x) {
    return ((x >> 8) & 0x00FFUL) | ((x << 8) & 0xFF00UL);
}

#if defined(__GNUC__) || defined(__clang__)

//! bswap16 - gcc/clang intrinsic
static inline std::uint16_t bswap16(const std::uint16_t& v) {
    return __builtin_bswap16(v);
}

#elif defined(_MSC_VER)

//! bswap16 - MSVC intrinsic
static inline std::uint16_t bswap16(const std::uint16_t& v) {
    return _byteswap_ushort(v);
}

#else

//! bswap16 - generic
static inline std::uint16_t bswap16(const std::uint16_t& v) {
    return bswap16_generic(v);
}

#endif

/******************************************************************************/
// bswap32() - swap 32-bit integers

//! bswap32 - generic implementation
static inline std::uint32_t bswap32_generic(const std::uint32_t& x) {
    return ((x >> 24) & 0x000000FFUL) | ((x << 24) & 0xFF000000UL) |
           ((x >> 8) & 0x0000FF00UL) | ((x << 8) & 0x00FF0000UL);
}

#if defined(__GNUC__) || defined(__clang__)

//! bswap32 - gcc/clang intrinsic
static inline std::uint32_t bswap32(const std::uint32_t& v) {
    return __builtin_bswap32(v);
}

#elif defined(_MSC_VER)

//! bswap32 - MSVC intrinsic
static inline std::uint32_t bswap32(const std::uint32_t& v) {
    return _byteswap_ulong(v);
}

#else

//! bswap32 - generic
static inline std::uint32_t bswap32(const std::uint32_t& v) {
    return bswap32_generic(v);
}

#endif

/******************************************************************************/
// bswap64() - swap 64-bit integers

//! bswap64 - generic implementation
static inline std::uint64_t bswap64_generic(const std::uint64_t& x) {
    return ((x >> 56) & 0x00000000000000FFull) |
           ((x >> 40) & 0x000000000000FF00ull) |
           ((x >> 24) & 0x0000000000FF0000ull) |
           ((x >> 8) & 0x00000000FF000000ull) |
           ((x << 8) & 0x000000FF00000000ull) |
           ((x << 24) & 0x0000FF0000000000ull) |
           ((x << 40) & 0x00FF000000000000ull) |
           ((x << 56) & 0xFF00000000000000ull);
}

#if defined(__GNUC__) || defined(__clang__)

//! bswap64 - gcc/clang intrinsic
static inline std::uint64_t bswap64(const std::uint64_t& v) {
    return __builtin_bswap64(v);
}

#elif defined(_MSC_VER)

//! bswap64 - MSVC intrinsic
static inline std::uint64_t bswap64(const std::uint64_t& v) {
    return _byteswap_uint64(v);
}

#else

//! bswap64 - generic
static inline std::uint64_t bswap64(const std::uint64_t& v) {
    return bswap64_generic(v);
}

#endif

/******************************************************************************/

//! \}

} // namespace tlx

#endif // !TLX_MATH_BSWAP_HEADER

/******************************************************************************/
