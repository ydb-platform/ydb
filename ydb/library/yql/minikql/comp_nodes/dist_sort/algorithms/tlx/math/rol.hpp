/*******************************************************************************
 * tlx/math/rol.hpp
 *
 * rol32() to rotate bits left - mainly for portability.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_MATH_ROL_HEADER
#define TLX_MATH_ROL_HEADER

#include <cstdint>

#ifdef _MSC_VER
#include <cstdlib>
#endif

namespace tlx {

//! \addtogroup tlx_math
//! \{

/******************************************************************************/
// rol32() - rotate bits left in 32-bit integers

//! rol32 - generic implementation
static inline std::uint32_t rol32_generic(const std::uint32_t& x, int i) {
    return (x << static_cast<std::uint32_t>(i & 31)) |
           (x >> static_cast<std::uint32_t>((32 - (i & 31)) & 31));
}

#if (defined(__GNUC__) || defined(__clang__)) && (defined(__i386__) || defined(__x86_64__))

//! rol32 - gcc/clang assembler
static inline std::uint32_t rol32(const std::uint32_t& x, int i) {
    std::uint32_t x1 = x;
    asm ("roll %%cl,%0" : "=r" (x1) : "0" (x1), "c" (i));
    return x1;
}

#elif defined(_MSC_VER)

//! rol32 - MSVC intrinsic
static inline std::uint32_t rol32(const std::uint32_t& x, int i) {
    return _rotl(x, i);
}

#else

//! rol32 - generic
static inline std::uint32_t rol32(const std::uint32_t& x, int i) {
    return rol32_generic(x, i);
}

#endif

/******************************************************************************/
// rol64() - rotate bits left in 64-bit integers

//! rol64 - generic implementation
static inline std::uint64_t rol64_generic(const std::uint64_t& x, int i) {
    return (x << static_cast<std::uint64_t>(i & 63)) |
           (x >> static_cast<std::uint64_t>((64 - (i & 63)) & 63));
}

#if (defined(__GNUC__) || defined(__clang__)) && defined(__x86_64__)

//! rol64 - gcc/clang assembler
static inline std::uint64_t rol64(const std::uint64_t& x, int i) {
    std::uint64_t x1 = x;
    asm ("rolq %%cl,%0" : "=r" (x1) : "0" (x1), "c" (i));
    return x1;
}

#elif defined(_MSC_VER)

//! rol64 - MSVC intrinsic
static inline std::uint64_t rol64(const std::uint64_t& x, int i) {
    return _rotl64(x, i);
}

#else

//! rol64 - generic
static inline std::uint64_t rol64(const std::uint64_t& x, int i) {
    return rol64_generic(x, i);
}

#endif

/******************************************************************************/

//! \}

} // namespace tlx

#endif // !TLX_MATH_ROL_HEADER

/******************************************************************************/
