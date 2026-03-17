#ifndef STAN_MATH_PRIM_SCAL_FUN_LOG_SUM_EXP_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LOG_SUM_EXP_HPP

#include <stan/math/prim/scal/fun/log1p_exp.hpp>
#include <boost/math/tools/promotion.hpp>
#include <limits>

namespace stan {
namespace math {

/**
 * Calculates the log sum of exponetials without overflow.
 *
 * \f$\log (\exp(a) + \exp(b)) = m + \log(\exp(a-m) + \exp(b-m))\f$,
 *
 * where \f$m = max(a, b)\f$.
 *
 *
   \f[
   \mbox{log\_sum\_exp}(x, y) =
   \begin{cases}
     \ln(\exp(x)+\exp(y)) & \mbox{if } -\infty\leq x, y \leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{log\_sum\_exp}(x, y)}{\partial x} =
   \begin{cases}
     \frac{\exp(x)}{\exp(x)+\exp(y)} & \mbox{if } -\infty\leq x, y \leq \infty
 \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{log\_sum\_exp}(x, y)}{\partial y} =
   \begin{cases}
     \frac{\exp(y)}{\exp(x)+\exp(y)} & \mbox{if } -\infty\leq x, y \leq \infty
 \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param a the first variable
 * @param b the second variable
 */
template <typename T1, typename T2>
inline typename boost::math::tools::promote_args<T1, T2>::type log_sum_exp(
    const T2& a, const T1& b) {
  using std::exp;
  if (a > b)
    return a + log1p_exp(b - a);
  return b + log1p_exp(a - b);
}

}  // namespace math
}  // namespace stan

#endif
