#ifndef STAN_MATH_PRIM_SCAL_FUN_LBETA_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LBETA_HPP

#include <boost/math/tools/promotion.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>

namespace stan {
namespace math {

/**
 * Return the log of the beta function applied to the specified
 * arguments.
 *
 * The beta function is defined for \f$a > 0\f$ and \f$b > 0\f$ by
 *
 * \f$\mbox{B}(a, b) = \frac{\Gamma(a) \Gamma(b)}{\Gamma(a+b)}\f$.
 *
 * This function returns its log,
 *
 * \f$\log \mbox{B}(a, b) = \log \Gamma(a) + \log \Gamma(b) - \log
 \Gamma(a+b)\f$.
 *
 * See stan::math::lgamma() for the double-based and stan::math for the
 * variable-based log Gamma function.
 *
 *
   \f[
   \mbox{lbeta}(\alpha, \beta) =
   \begin{cases}
     \ln\int_0^1 u^{\alpha - 1} (1 - u)^{\beta - 1} \, du & \mbox{if } \alpha,
 \beta>0 \\[6pt] \textrm{NaN} & \mbox{if } \alpha = \textrm{NaN or } \beta =
 \textrm{NaN} \end{cases} \f]

   \f[
   \frac{\partial\, \mbox{lbeta}(\alpha, \beta)}{\partial \alpha} =
   \begin{cases}
     \Psi(\alpha)-\Psi(\alpha+\beta) & \mbox{if } \alpha, \beta>0 \\[6pt]
     \textrm{NaN} & \mbox{if } \alpha = \textrm{NaN or } \beta = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{lbeta}(\alpha, \beta)}{\partial \beta} =
   \begin{cases}
     \Psi(\beta)-\Psi(\alpha+\beta) & \mbox{if } \alpha, \beta>0 \\[6pt]
     \textrm{NaN} & \mbox{if } \alpha = \textrm{NaN or } \beta = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param a First value
 * @param b Second value
 * @return Log of the beta function applied to the two values.
 * @tparam T1 Type of first value.
 * @tparam T2 Type of second value.
 */
template <typename T1, typename T2>
inline typename boost::math::tools::promote_args<T1, T2>::type lbeta(
    const T1 a, const T2 b) {
  return lgamma(a) + lgamma(b) - lgamma(a + b);
}

}  // namespace math
}  // namespace stan

#endif
