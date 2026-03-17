#ifndef STAN_MATH_PRIM_SCAL_FUN_IS_NAN_HPP
#define STAN_MATH_PRIM_SCAL_FUN_IS_NAN_HPP

#include <cmath>

namespace stan {
namespace math {

/**
 * Returns true if the input is NaN and false otherwise.
 *
 * Delegates to <code>std::isnan</code>.
 *
 * @param x Value to test.
 * @return <code>true</code> if the value is NaN.
 */
inline bool is_nan(double x) { return std::isnan(x); }

}  // namespace math
}  // namespace stan

#endif
