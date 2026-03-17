#ifndef STAN_MATH_REV_SCAL_FUN_FMIN_HPP
#define STAN_MATH_REV_SCAL_FUN_FMIN_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/meta/likely.hpp>
#include <stan/math/rev/scal/fun/is_nan.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>

namespace stan {
namespace math {

/**
 * Returns the minimum of the two variable arguments (C99).
 *
 * For <code>fmin(a, b)</code>, if a's value is less than b's,
 * then a is returned, otherwise b is returned.
 *
   \f[
   \mbox{fmin}(x, y) =
   \begin{cases}
     x & \mbox{if } x \leq y \\
     y & \mbox{if } x > y \\[6pt]
     x & \mbox{if } -\infty\leq x\leq \infty, y = \textrm{NaN}\\
     y & \mbox{if } -\infty\leq y\leq \infty, x = \textrm{NaN}\\
     \textrm{NaN} & \mbox{if } x, y = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{fmin}(x, y)}{\partial x} =
   \begin{cases}
     1 & \mbox{if } x \leq y \\
     0 & \mbox{if } x > y \\[6pt]
     1 & \mbox{if } -\infty\leq x\leq \infty, y = \textrm{NaN}\\
     0 & \mbox{if } -\infty\leq y\leq \infty, x = \textrm{NaN}\\
     \textrm{NaN} & \mbox{if } x, y = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{fmin}(x, y)}{\partial y} =
   \begin{cases}
     0 & \mbox{if } x \leq y \\
     1 & \mbox{if } x > y \\[6pt]
     0 & \mbox{if } -\infty\leq x\leq \infty, y = \textrm{NaN}\\
     1 & \mbox{if } -\infty\leq y\leq \infty, x = \textrm{NaN}\\
     \textrm{NaN} & \mbox{if } x, y = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param a First variable.
 * @param b Second variable.
 * @return If the first variable's value is smaller than the
 * second's, the first variable, otherwise the second variable.
 */
inline var fmin(const var& a, const var& b) {
  if (unlikely(is_nan(a))) {
    if (unlikely(is_nan(b)))
      return var(new precomp_vv_vari(NOT_A_NUMBER, a.vi_, b.vi_, NOT_A_NUMBER,
                                     NOT_A_NUMBER));
    return b;
  }
  if (unlikely(is_nan(b)))
    return a;
  return a < b ? a : b;
}

/**
 * Returns the minimum of the variable and scalar, promoting the
 * scalar to a variable if it is larger (C99).
 *
 * For <code>fmin(a, b)</code>, if a's value is less than or equal
 * to b, then a is returned, otherwise a fresh variable wrapping b
 * is returned.
 *
 * @param a First variable.
 * @param b Second value
 * @return If the first variable's value is less than or equal to the second
 * value, the first variable, otherwise the second value promoted to a fresh
 * variable.
 */
inline var fmin(const var& a, double b) {
  if (unlikely(is_nan(a))) {
    if (unlikely(is_nan(b)))
      return var(new precomp_v_vari(NOT_A_NUMBER, a.vi_, NOT_A_NUMBER));
    return var(b);
  }
  if (unlikely(is_nan(b)))
    return a;
  return a <= b ? a : var(b);
}

/**
 * Returns the minimum of a scalar and variable, promoting the scalar to
 * a variable if it is larger (C99).
 *
 * For <code>fmin(a, b)</code>, if a is less than b's value, then a
 * fresh variable implementation wrapping a is returned, otherwise
 * b is returned.
 *
 * @param a First value.
 * @param b Second variable.
 * @return If the first value is smaller than the second variable's value,
 * return the first value promoted to a variable, otherwise return the
 * second variable.
 */
inline var fmin(double a, const var& b) {
  if (unlikely(is_nan(b))) {
    if (unlikely(is_nan(a)))
      return var(new precomp_v_vari(NOT_A_NUMBER, b.vi_, NOT_A_NUMBER));
    return var(a);
  }
  if (unlikely(is_nan(a)))
    return b;
  return b <= a ? b : var(a);
}

}  // namespace math
}  // namespace stan
#endif
