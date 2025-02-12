/*******************************************************************************
 * tlx/math/abs_diff.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_MATH_ABS_DIFF_HEADER
#define TLX_MATH_ABS_DIFF_HEADER

namespace tlx {

//! \addtogroup tlx_math
//! \{

/******************************************************************************/
// abs_diff() - calculate absolute difference

//! absolute difference, which also works for unsigned types
template <typename T>
T abs_diff(const T& a, const T& b) {
    return a > b ? a - b : b - a;
}

//! \}

} // namespace tlx

#endif // !TLX_MATH_ABS_DIFF_HEADER

/******************************************************************************/
