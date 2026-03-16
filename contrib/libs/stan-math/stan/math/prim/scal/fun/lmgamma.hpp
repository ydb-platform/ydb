#ifndef STAN_MATH_PRIM_SCAL_FUN_LMGAMMA_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LMGAMMA_HPP

#include <boost/math/tools/promotion.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>

namespace stan {
namespace math {

/**
 * Return the natural logarithm of the multivariate gamma function
 * with the speciifed dimensions and argument.
 *
 * <p>The multivariate gamma function \f$\Gamma_k(x)\f$ for
 * dimensionality \f$k\f$ and argument \f$x\f$ is defined by
 *
 * <p>\f$\Gamma_k(x) = \pi^{k(k-1)/4} \, \prod_{j=1}^k \Gamma(x + (1 - j)/2)\f$,
 *
 * where \f$\Gamma()\f$ is the gamma function.
 *
 *
   \f[
   \mbox{lmgamma}(n, x) =
   \begin{cases}
     \textrm{error} & \mbox{if } x\in \{\dots, -3, -2, -1, 0\}\\
     \ln\Gamma_n(x) & \mbox{if } x\not\in \{\dots, -3, -2, -1, 0\}\\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{lmgamma}(n, x)}{\partial x} =
   \begin{cases}
     \textrm{error} & \mbox{if } x\in \{\dots, -3, -2, -1, 0\}\\
     \frac{\partial\, \ln\Gamma_n(x)}{\partial x} & \mbox{if } x\not\in \{\dots,
 -3, -2, -1, 0\}\\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN} \end{cases}
   \f]

   \f[
   \ln\Gamma_n(x) = \pi^{n(n-1)/4} \, \prod_{j=1}^n \Gamma(x + (1 - j)/2)
   \f]

   \f[
   \frac{\partial \, \ln\Gamma_n(x)}{\partial x} = \sum_{j=1}^n \Psi(x + (1 - j)
 / 2) \f]
 *
 * @param k Number of dimensions.
 * @param x Function argument.
 * @return Natural log of the multivariate gamma function.
 * @tparam T Type of scalar.
 */
template <typename T>
inline typename boost::math::tools::promote_args<T>::type lmgamma(int k, T x) {
  typename boost::math::tools::promote_args<T>::type result
      = k * (k - 1) * LOG_PI_OVER_FOUR;

  for (int j = 1; j <= k; ++j)
    result += lgamma(x + (1.0 - j) / 2.0);
  return result;
}

}  // namespace math
}  // namespace stan
#endif
