#ifndef STAN_MATH_PRIM_SCAL_FUN_LOGICAL_OR_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LOGICAL_OR_HPP

namespace stan {
namespace math {

/**
 * The logical or function which returns 1 if either
 * argument is unequal to zero and 0 otherwise.  Equivalent
 * to <code>x1 != 0 || x2 != 0</code>.
 *
   \f[
   \mbox{operator||}(x, y) =
   \begin{cases}
     0 & \mbox{if } x, y=0 \\
     1 & \mbox{if } x \neq 0 \textrm{ or } y\neq0\\[6pt]
     1 & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @tparam T1 Type of first argument.
 * @tparam T2 Type of second argument.
 * @param x1 First argument
 * @param x2 Second argument
 * @return <code>true</code> if either x1 or x2 is not equal to 0.
 */
template <typename T1, typename T2>
inline int logical_or(T1 x1, T2 x2) {
  return (x1 != 0) || (x2 != 0);
}

}  // namespace math
}  // namespace stan

#endif
