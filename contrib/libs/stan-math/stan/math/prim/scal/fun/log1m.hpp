#ifndef STAN_MATH_PRIM_SCAL_FUN_LOG1M_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LOG1M_HPP

#include <stan/math/prim/scal/fun/log1p.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <stan/math/prim/scal/err/check_less_or_equal.hpp>

namespace stan {
namespace math {

/**
 * Return the natural logarithm of one minus the specified value.
 *
 * The main use of this function is to cut down on intermediate
 * values during algorithmic differentiation.
 *
 *
   \f[
   \mbox{log1m}(x) =
   \begin{cases}
     \ln(1-x) & \mbox{if } x \leq 1 \\
     \textrm{NaN} & \mbox{if } x > 1\\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{log1m}(x)}{\partial x} =
   \begin{cases}
     -\frac{1}{1-x} & \mbox{if } x \leq 1 \\
     \textrm{NaN} & \mbox{if } x > 1\\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param[in] x Argument.
 * @return Natural log of one minus the argument.
 * @throw <code>std::domain_error</code> If the argument is greater than 1.
 * @throw <code>std::overflow_error</code> If the computation overflows.
 */
inline double log1m(double x) {
  if (!is_nan(x))
    check_less_or_equal("log1m", "x", x, 1);
  return stan::math::log1p(-x);
}

}  // namespace math
}  // namespace stan

#endif
