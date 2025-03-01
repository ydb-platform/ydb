/*******************************************************************************
 * tlx/math/ror.hpp
 *
 * ror32() to rotate bits right - mainly for portability.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_MATH_ROR_HEADER
#define TLX_MATH_ROR_HEADER

#include <cstdint>

#ifdef _MSC_VER
#include <cstdlib>
#endif

namespace tlx {

//! \addtogroup tlx_math
//! \{

/******************************************************************************/
// ror32() - rotate bits right in 32-bit integers

//! ror32 - generic implementation
static inline std::uint32_t ror32_generic(const std::uint32_t& x, int i) {
    return (x >> static_cast<std::uint32_t>(i & 31)) |
           (x << static_cast<std::uint32_t>((32 - (i & 31)) & 31));
}

#if (defined(__GNUC__) || defined(__clang__)) && (defined(__i386__) || defined(__x86_64__))

//! ror32 - gcc/clang assembler
static inline std::uint32_t ror32(const std::uint32_t& x, int i) {
    std::uint32_t x1 = x;
    asm ("rorl %%cl,%0" : "=r" (x1) : "0" (x1), "c" (i));
    return x1;
}

#elif defined(_MSC_VER)

//! ror32 - MSVC intrinsic
static inline std::uint32_t ror32(const std::uint32_t& x, int i) {
    return _rotr(x, i);
}

#else

//! ror32 - generic
static inline std::uint32_t ror32(const std::uint32_t& x, int i) {
    return ror32_generic(x, i);
}

#endif

/******************************************************************************/
// ror64() - rotate bits right in 64-bit integers

//! ror64 - generic implementation
static inline std::uint64_t ror64_generic(const std::uint64_t& x, int i) {
    return (x >> static_cast<std::uint64_t>(i & 63)) |
           (x << static_cast<std::uint64_t>((64 - (i & 63)) & 63));
}

#if (defined(__GNUC__) || defined(__clang__)) && defined(__x86_64__)

//! ror64 - gcc/clang assembler
static inline std::uint64_t ror64(const std::uint64_t& x, int i) {
    std::uint64_t x1 = x;
    asm ("rorq %%cl,%0" : "=r" (x1) : "0" (x1), "c" (i));
    return x1;
}

#elif defined(_MSC_VER)

//! ror64 - MSVC intrinsic
static inline std::uint64_t ror64(const std::uint64_t& x, int i) {
    return _rotr64(x, i);
}

#else

//! ror64 - generic
static inline std::uint64_t ror64(const std::uint64_t& x, int i) {
    return ror64_generic(x, i);
}

#endif

/******************************************************************************/

//! \}

} // namespace tlx

#endif // !TLX_MATH_ROR_HEADER

/******************************************************************************/
