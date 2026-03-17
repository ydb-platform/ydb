#ifndef STAN_MATH_PRIM_SCAL_FUN_ROUND_HPP
#define STAN_MATH_PRIM_SCAL_FUN_ROUND_HPP

#include <cmath>

namespace stan {
namespace math {

/**
 * Return the closest integer to the specified argument, with
 * halfway cases rounded away from zero.
 *
 * @param x Argument.
 * @return The rounded value of the argument.
 */
inline double round(double x) { return std::round(x); }

/**
 * Return the closest integer to the specified argument, with
 * halfway cases rounded away from zero.
 *
 * @param x Argument.
 * @return The rounded value of the argument.
 */
inline double round(int x) { return std::round(x); }

}  // namespace math
}  // namespace stan
#endif
