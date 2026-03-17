#ifndef STAN_MATH_PRIM_SCAL_FUN_TRUNC_HPP
#define STAN_MATH_PRIM_SCAL_FUN_TRUNC_HPP

#include <cmath>

namespace stan {
namespace math {

/**
 * Return the nearest integral value that is not larger in
 * magnitude than the specified argument.
 *
 * @param[in] x Argument.
 * @return The truncated argument.
 */
inline double trunc(double x) { return std::trunc(x); }

/**
 * Return the nearest integral value that is not larger in
 * magnitude than the specified argument.
 *
 * @param[in] x Argument.
 * @return The truncated argument.
 */
inline double trunc(int x) { return std::trunc(x); }

}  // namespace math
}  // namespace stan
#endif
