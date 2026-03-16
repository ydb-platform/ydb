#ifndef STAN_MATH_PRIM_SCAL_FUN_IS_INF_HPP
#define STAN_MATH_PRIM_SCAL_FUN_IS_INF_HPP

#include <cmath>

namespace stan {
namespace math {

/**
 * Returns true if the input is infinite and false otherwise.
 *
 * Delegates to <code>std::isinf</code>.
 *
 * @param x Value to test.
 * @return <code>true</code> if the value is infinite.
 */
inline bool is_inf(double x) { return std::isinf(x); }

}  // namespace math
}  // namespace stan

#endif
