/*******************************************************************************
 * tlx/math/integer_log2.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_MATH_INTEGER_LOG2_HEADER
#define TLX_MATH_INTEGER_LOG2_HEADER

#include <tlx/define/constexpr.hpp>

namespace tlx {

//! \addtogroup tlx_math
//! \{

/******************************************************************************/
// integer_log2_floor()

//! calculate the log2 floor of an integer type
template <typename IntegerType>
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_floor_template(IntegerType i) {
    unsigned p = 0;
    while (i >= 65536) i >>= 16, p += 16;
    while (i >= 256) i >>= 8, p += 8;
    while (i >>= 1) ++p;
    return p;
}

/******************************************************************************/
// integer_log2_floor()

#if defined(__GNUC__) || defined(__clang__)

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_floor(int i) {
    if (i == 0) return 0;
    return 8 * sizeof(int) - 1 - __builtin_clz(i);
}

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_floor(unsigned int i) {
    if (i == 0) return 0;
    return 8 * sizeof(unsigned) - 1 - __builtin_clz(i);
}

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_floor(long i) {
    if (i == 0) return 0;
    return 8 * sizeof(long) - 1 - __builtin_clzl(i);
}

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_floor(unsigned long i) {
    if (i == 0) return 0;
    return 8 * sizeof(unsigned long) - 1 - __builtin_clzl(i);
}

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_floor(long long i) {
    if (i == 0) return 0;
    return 8 * sizeof(long long) - 1 - __builtin_clzll(i);
}

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_floor(unsigned long long i) {
    if (i == 0) return 0;
    return 8 * sizeof(unsigned long long) - 1 - __builtin_clzll(i);
}

#else

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_floor(int i) {
    return integer_log2_floor_template(i);
}

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_floor(unsigned int i) {
    return integer_log2_floor_template(i);
}

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_floor(long i) {
    return integer_log2_floor_template(i);
}

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_floor(unsigned long i) {
    return integer_log2_floor_template(i);
}

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_floor(long long i) {
    return integer_log2_floor_template(i);
}

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_floor(unsigned long long i) {
    return integer_log2_floor_template(i);
}

#endif

/******************************************************************************/
// integer_log2_ceil()

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_ceil(int i) {
    if (i <= 1) return 0;
    return integer_log2_floor(i - 1) + 1;
}

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_ceil(unsigned int i) {
    if (i <= 1) return 0;
    return integer_log2_floor(i - 1) + 1;
}

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_ceil(long i) {
    if (i <= 1) return 0;
    return integer_log2_floor(i - 1) + 1;
}

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_ceil(unsigned long i) {
    if (i <= 1) return 0;
    return integer_log2_floor(i - 1) + 1;
}

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_ceil(long long i) {
    if (i <= 1) return 0;
    return integer_log2_floor(i - 1) + 1;
}

//! calculate the log2 floor of an integer type
static TLX_ADVANCED_CONSTEXPR unsigned integer_log2_ceil(unsigned long long i) {
    if (i <= 1) return 0;
    return integer_log2_floor(i - 1) + 1;
}

//! \}

} // namespace tlx

#endif // !TLX_MATH_INTEGER_LOG2_HEADER

/******************************************************************************/
