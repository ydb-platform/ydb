#ifndef STAN_MATH_PRIM_SCAL_FUN_STEP_HPP
#define STAN_MATH_PRIM_SCAL_FUN_STEP_HPP

namespace stan {
namespace math {

/**
 * The step, or Heaviside, function.
 *
 * The function is defined by
 *
 * <code>step(y) = (y < 0.0) ? 0 : 1</code>.
 *
   \f[
   \mbox{step}(x) =
   \begin{cases}
     0 & \mbox{if } x \leq 0 \\
     1 & \mbox{if } x > 0  \\[6pt]
     0 & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @tparam T type of value
 * @param y value
 * @return zero if the value is less than zero, and one otherwise
 */
template <typename T>
inline double step(const T& y) {
  return y < 0.0 ? 0 : 1;
}

}  // namespace math
}  // namespace stan

#endif
