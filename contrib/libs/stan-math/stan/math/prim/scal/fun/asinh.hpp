#ifndef STAN_MATH_PRIM_SCAL_FUN_ASINH_HPP
#define STAN_MATH_PRIM_SCAL_FUN_ASINH_HPP

#include <cmath>

namespace stan {
namespace math {

/**
 * Return the inverse hyperbolic sine of the specified value.
 * Returns infinity for infinity argument and -infinity for
 * -infinity argument.
 * Returns nan for nan argument.
 *
 * @param[in] x Argument.
 * @return Inverse hyperbolic sine of the argument.
 */
inline double asinh(double x) { return std::asinh(x); }

/**
 * Integer version of asinh.
 *
 * @param[in] x Argument.
 * @return Inverse hyperbolic sine of the argument.
 */
inline double asinh(int x) { return std::asinh(x); }

}  // namespace math
}  // namespace stan
#endif
