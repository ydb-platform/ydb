#ifndef STAN_MATH_PRIM_SCAL_FUN_LOG_INV_LOGIT_DIFF_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LOG_INV_LOGIT_DIFF_HPP

#include <stan/math/prim/scal/fun/log1m_exp.hpp>
#include <stan/math/prim/scal/fun/log1p_exp.hpp>
#include <boost/math/tools/promotion.hpp>

namespace stan {
namespace math {

/**
 * Returns the natural logarithm of the difference of the
 * inverse logits of the specified arguments.
 *
   \f[
     \mathrm{log\_inv\_logit\_diff}(x,y) =
      \ln\left(\frac{1}{1+\exp(-x)}-\frac{1}{1+\exp(-y)}\right)
   \f]

   \f[
    \frac{\partial }{\partial x} = -\frac{e^x}{e^y-e^x}-\frac{e^x}{e^x+1}
   \f]

   \f[
    \frac{\partial }{\partial x} = -\frac{e^y}{e^x-e^y}-\frac{e^y}{e^y+1}
   \f]
 *
 * @tparam T1 Type of x argument.
 * @tparam T2 Type of y argument.
 * @param x Argument.
 * @param y Argument.
 * @return Result of log difference of inverse logits of arguments.
 */
template <typename T1, typename T2>
inline typename boost::math::tools::promote_args<T1, T2>::type
log_inv_logit_diff(const T1& x, const T2& y) {
  return x - log1p_exp(x) + log1m_exp(y - x) - log1p_exp(y);
}

}  // namespace math
}  // namespace stan

#endif
