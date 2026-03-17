#ifndef STAN_MATH_PRIM_SCAL_FUN_SUM_HPP
#define STAN_MATH_PRIM_SCAL_FUN_SUM_HPP

namespace stan {
namespace math {

/**
 * Returns specified input value.
 *
 * @tparam T Type of element.
 * @param v Specified value.
 * @return Same value (the sum of one value).
 */
inline double sum(double v) { return v; }

/**
 * Returns specified input value.
 *
 * @tparam T Type of element.
 * @param v Specified value.
 * @return Same value (the sum of one value).
 */
inline int sum(int v) { return v; }

}  // namespace math
}  // namespace stan
#endif
