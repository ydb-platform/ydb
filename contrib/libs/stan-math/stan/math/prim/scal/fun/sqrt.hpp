#ifndef STAN_MATH_PRIM_SCAL_FUN_SQRT_HPP
#define STAN_MATH_PRIM_SCAL_FUN_SQRT_HPP

#include <cmath>

namespace stan {
namespace math {

/**
 * Return the square root of the specified argument.  This
 * version is required to disambiguate <code>sqrt(int)</code>.
 *
 * @param[in] x Argument.
 * @return Natural exponential of argument.
 */
inline double sqrt(int x) { return std::sqrt(x); }

}  // namespace math
}  // namespace stan
#endif
