#ifndef STAN_MATH_PRIM_SCAL_FUN_LOG_INV_LOGIT_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LOG_INV_LOGIT_HPP

#include <stan/math/prim/scal/fun/log1p.hpp>
#include <boost/math/tools/promotion.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Returns the natural logarithm of the inverse logit of the
 * specified argument.
 *
   \f[
   \mbox{log\_inv\_logit}(x) =
   \begin{cases}
     \ln\left(\frac{1}{1+\exp(-x)}\right)& \mbox{if } -\infty\leq x \leq \infty
 \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN} \end{cases} \f]

   \f[
   \frac{\partial\, \mbox{log\_inv\_logit}(x)}{\partial x} =
   \begin{cases}
     \frac{1}{1+\exp(x)} & \mbox{if } -\infty\leq x\leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param u argument
 * @return log of the inverse logit of argument
 */
inline double log_inv_logit(double u) {
  using std::exp;
  if (u < 0.0)
    return u - log1p(exp(u));  // prevent underflow
  return -log1p(exp(-u));
}

/**
 * Returns the natural logarithm of the inverse logit of the
 * specified argument.
 *
 * @param u argument
 * @return log of the inverse logit of argument
 */
inline double log_inv_logit(int u) {
  return log_inv_logit(static_cast<double>(u));
}

}  // namespace math
}  // namespace stan

#endif
