#ifndef STAN_MATH_PRIM_SCAL_FUN_EXP_HPP
#define STAN_MATH_PRIM_SCAL_FUN_EXP_HPP

#include <cmath>

namespace stan {
namespace math {

/**
 * Return the natural exponential of the specified argument.  This
 * version is required to disambiguate <code>exp(int)</code>.
 *
 * @param[in] x Argument.
 * @return Natural exponential of argument.
 */
inline double exp(int x) { return std::exp(x); }

}  // namespace math
}  // namespace stan
#endif
