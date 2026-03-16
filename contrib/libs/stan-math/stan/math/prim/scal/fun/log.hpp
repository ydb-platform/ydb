#ifndef STAN_MATH_PRIM_SCAL_FUN_LOG_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LOG_HPP

#include <cmath>

namespace stan {
namespace math {

/**
 * Return the natural log of the specified argument.  This version
 * is required to disambiguate <code>log(int)</code>.
 *
 * @param[in] x Argument.
 * @return Natural log of argument.
 */
inline double log(int x) { return std::log(x); }

}  // namespace math
}  // namespace stan
#endif
