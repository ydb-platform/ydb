/*******************************************************************************
 * tlx/math.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_MATH_HEADER
#define TLX_MATH_HEADER

//! \defgroup tlx_math Math Functions
//! Simple math functions

/*[[[perl
print "#include <$_>\n" foreach sort glob("tlx/math/"."*.hpp");
]]]*/
#include <tlx/math/abs_diff.hpp>
#include <tlx/math/aggregate.hpp>
#include <tlx/math/aggregate_min_max.hpp>
#include <tlx/math/bswap.hpp>
#include <tlx/math/bswap_be.hpp>
#include <tlx/math/bswap_le.hpp>
#include <tlx/math/clz.hpp>
#include <tlx/math/ctz.hpp>
#include <tlx/math/div_ceil.hpp>
#include <tlx/math/ffs.hpp>
#include <tlx/math/integer_log2.hpp>
#include <tlx/math/is_power_of_two.hpp>
#include <tlx/math/polynomial_regression.hpp>
#include <tlx/math/popcount.hpp>
#include <tlx/math/power_to_the.hpp>
#include <tlx/math/rol.hpp>
#include <tlx/math/ror.hpp>
#include <tlx/math/round_to_power_of_two.hpp>
#include <tlx/math/round_up.hpp>
#include <tlx/math/sgn.hpp>
// [[[end]]]

#endif // !TLX_MATH_HEADER

/******************************************************************************/
