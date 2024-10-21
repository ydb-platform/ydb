/*******************************************************************************
 * tlx/math/is_power_of_two.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_MATH_IS_POWER_OF_TWO_HEADER
#define TLX_MATH_IS_POWER_OF_TWO_HEADER

namespace tlx {

//! \addtogroup tlx_math
//! \{

/******************************************************************************/

template <typename Integral>
static inline bool is_power_of_two_template(Integral i) {
    if (i <= 0) return false;
    return !(i & (i - 1));
}

/******************************************************************************/
// is_power_of_two()

//! does what it says: true if i is a power of two
static inline bool is_power_of_two(int i) {
    return is_power_of_two_template(i);
}

//! does what it says: true if i is a power of two
static inline bool is_power_of_two(unsigned int i) {
    return is_power_of_two_template(i);
}

//! does what it says: true if i is a power of two
static inline bool is_power_of_two(long i) {
    return is_power_of_two_template(i);
}

//! does what it says: true if i is a power of two
static inline bool is_power_of_two(unsigned long i) {
    return is_power_of_two_template(i);
}

//! does what it says: true if i is a power of two
static inline bool is_power_of_two(long long i) {
    return is_power_of_two_template(i);
}

//! does what it says: true if i is a power of two
static inline bool is_power_of_two(unsigned long long i) {
    return is_power_of_two_template(i);
}

//! \}

} // namespace tlx

#endif // !TLX_MATH_IS_POWER_OF_TWO_HEADER

/******************************************************************************/
