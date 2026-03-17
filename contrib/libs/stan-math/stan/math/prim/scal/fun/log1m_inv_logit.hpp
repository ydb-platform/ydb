#ifndef STAN_MATH_PRIM_SCAL_FUN_LOG1M_INV_LOGIT_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LOG1M_INV_LOGIT_HPP

#include <stan/math/prim/scal/fun/log1p.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Returns the natural logarithm of 1 minus the inverse logit
 * of the specified argument.
 *
 *
   \f[
   \mbox{log1m\_inv\_logit}(x) =
   \begin{cases}
     -\ln(\exp(x)+1) & \mbox{if } -\infty\leq x \leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{log1m\_inv\_logit}(x)}{\partial x} =
   \begin{cases}
     -\frac{\exp(x)}{\exp(x)+1} & \mbox{if } -\infty\leq x\leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param u argument
 * @return log of one minus the inverse logit of the argument
 */
inline double log1m_inv_logit(double u) {
  using std::exp;
  if (u > 0.0)
    return -u - log1p(exp(-u));  // prevent underflow
  return -log1p(exp(u));
}

/**
 * Return the natural logarithm of one minus the inverse logit of
 * the specified argument.
 *
 * @param u argument
 * @return log of one minus the inverse logit of the argument
 */
inline double log1m_inv_logit(int u) {
  return log1m_inv_logit(static_cast<double>(u));
}

}  // namespace math
}  // namespace stan
#endif
