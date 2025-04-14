/*******************************************************************************
 * tlx/math/sgn.hpp
 *
 * sgn() return the signum (-1, 0, +1) of a value.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_MATH_SGN_HEADER
#define TLX_MATH_SGN_HEADER

namespace tlx {

//! \addtogroup tlx_math
//! \{

/******************************************************************************/
//! sgn() - signum

//! return the signum (-1, 0, +1) of a value.
template <typename T>
int sgn(const T& val) {
    // from https://stackoverflow.com/questions/1903954
    return (T(0) < val) - (val < T(0));
}

//! \}

} // namespace tlx

#endif // !TLX_MATH_SGN_HEADER

/******************************************************************************/
