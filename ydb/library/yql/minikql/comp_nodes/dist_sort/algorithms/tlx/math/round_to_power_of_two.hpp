/*******************************************************************************
 * tlx/math/round_to_power_of_two.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_MATH_ROUND_TO_POWER_OF_TWO_HEADER
#define TLX_MATH_ROUND_TO_POWER_OF_TWO_HEADER

#include <cstddef>

namespace tlx {

//! \addtogroup tlx_math
//! \{

/******************************************************************************/
// round_up_to_power_of_two()

template <typename Integral>
static inline Integral round_up_to_power_of_two_template(Integral n) {
    --n;
    for (size_t k = 1; k != 8 * sizeof(n); k <<= 1) {
        n |= n >> k;
    }
    ++n;
    return n;
}

/******************************************************************************/
// round_up_to_power_of_two()

//! does what it says: round up to next power of two
static inline int round_up_to_power_of_two(int i) {
    return round_up_to_power_of_two_template(i);
}

//! does what it says: round up to next power of two
static inline unsigned int round_up_to_power_of_two(unsigned int i) {
    return round_up_to_power_of_two_template(i);
}

//! does what it says: round up to next power of two
static inline long round_up_to_power_of_two(long i) {
    return round_up_to_power_of_two_template(i);
}

//! does what it says: round up to next power of two
static inline unsigned long round_up_to_power_of_two(unsigned long i) {
    return round_up_to_power_of_two_template(i);
}

//! does what it says: round up to next power of two
static inline long long round_up_to_power_of_two(long long i) {
    return round_up_to_power_of_two_template(i);
}

//! does what it says: round up to next power of two
static inline
unsigned long long round_up_to_power_of_two(unsigned long long i) {
    return round_up_to_power_of_two_template(i);
}

/******************************************************************************/
// round_down_to_power_of_two()

//! does what it says: round down to next power of two
static inline int round_down_to_power_of_two(int i) {
    return round_up_to_power_of_two(i + 1) >> 1;
}

//! does what it says: round down to next power of two
static inline unsigned int round_down_to_power_of_two(unsigned int i) {
    return round_up_to_power_of_two(i + 1) >> 1;
}

//! does what it says: round down to next power of two
static inline long round_down_to_power_of_two(long i) {
    return round_up_to_power_of_two(i + 1) >> 1;
}

//! does what it says: round down to next power of two
static inline unsigned long round_down_to_power_of_two(unsigned long i) {
    return round_up_to_power_of_two(i + 1) >> 1;
}

//! does what it says: round down to next power of two
static inline long long round_down_to_power_of_two(long long i) {
    return round_up_to_power_of_two(i + 1) >> 1;
}

//! does what it says: round down to next power of two
static inline
unsigned long long round_down_to_power_of_two(unsigned long long i) {
    return round_up_to_power_of_two(i + 1) >> 1;
}

//! \}

} // namespace tlx

#endif // !TLX_MATH_ROUND_TO_POWER_OF_TWO_HEADER

/******************************************************************************/
