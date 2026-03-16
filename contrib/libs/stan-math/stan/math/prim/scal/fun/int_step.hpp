#ifndef STAN_MATH_PRIM_SCAL_FUN_INT_STEP_HPP
#define STAN_MATH_PRIM_SCAL_FUN_INT_STEP_HPP

namespace stan {
namespace math {

/**
 * The integer step, or Heaviside, function.
 *
 * For double NaN input, int_step(NaN) returns 0.
 *
 * \f[
     \mbox{int\_step}(x) =
     \begin{cases}
       0 & \mbox{if } x \leq 0 \\
       1 & \mbox{if } x > 0 \\[6pt]
       0 & \mbox{if } x = \textrm{NaN}
     \end{cases}
     \f]
 *
 * @tparam T value type
 * @param[in] y value
 * @return 1 if value is greater than 0 and 0 otherwise
 */
template <typename T>
inline int int_step(const T& y) {
  return y > 0;
}

}  // namespace math
}  // namespace stan
#endif
