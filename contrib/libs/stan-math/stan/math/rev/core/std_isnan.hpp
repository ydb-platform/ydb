#ifndef STAN_MATH_REV_CORE_STD_ISNAN_HPP
#define STAN_MATH_REV_CORE_STD_ISNAN_HPP

#include <stan/math/rev/core/var.hpp>
#include <cmath>

namespace std {

/**
 * Checks if the given number is NaN.
 *
 * Return <code>true</code> if the value of the
 * specified variable is not a number.
 *
 * @param a Variable to test.
 * @return <code>true</code> if value is not a number.
 */
inline int isnan(const stan::math::var& a) { return isnan(a.val()); }

}  // namespace std
#endif
