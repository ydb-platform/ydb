#ifndef STAN_MATH_PRIM_SCAL_FUN_CBRT_HPP
#define STAN_MATH_PRIM_SCAL_FUN_CBRT_HPP

#include <cmath>

namespace stan {
namespace math {

/**
 * Return the cube root of the specified value
 *
 * @param[in] x Argument.
 * @return Cube root of the argument.
 * @throw std::domain_error If argument is negative.
 */
inline double cbrt(double x) { return std::cbrt(x); }

/**
 * Integer version of cbrt.
 *
 * @param[in] x Argument.
 * @return Cube root of the argument.
 * @throw std::domain_error If argument is less than 1.
 */
inline double cbrt(int x) { return std::cbrt(x); }

}  // namespace math
}  // namespace stan
#endif
