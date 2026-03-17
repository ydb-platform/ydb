#ifndef STAN_MATH_PRIM_SCAL_FUN_DIGAMMA_HPP
#define STAN_MATH_PRIM_SCAL_FUN_DIGAMMA_HPP

#include <stan/math/prim/scal/fun/boost_policy.hpp>
#include <boost/math/special_functions/digamma.hpp>

namespace stan {
namespace math {

/**
 * Return the derivative of the log gamma function
 * at the specified value.
 *
   \f[
   \mbox{digamma}(x) =
   \begin{cases}
     \textrm{error} & \mbox{if } x\in \{\dots, -3, -2, -1, 0\}\\
     \Psi(x) & \mbox{if } x\not\in \{\dots, -3, -2, -1, 0\}\\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{digamma}(x)}{\partial x} =
   \begin{cases}
     \textrm{error} & \mbox{if } x\in \{\dots, -3, -2, -1, 0\}\\
     \frac{\partial\, \Psi(x)}{\partial x} & \mbox{if } x\not\in \{\dots, -3,
 -2, -1, 0\}\\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN} \end{cases} \f]

   \f[
   \Psi(x)=\frac{\Gamma'(x)}{\Gamma(x)}
   \f]

   \f[
   \frac{\partial \, \Psi(x)}{\partial x} =
 \frac{\Gamma''(x)\Gamma(x)-(\Gamma'(x))^2}{\Gamma^2(x)} \f]

 *
 * The design follows the standard C++ library in returning NaN
 * rather than throwing exceptions.
 *
 * @param[in] x argument
 * @return derivative of log gamma function at argument
 */
inline double digamma(double x) {
  return boost::math::digamma(x, boost_policy_t());
}

}  // namespace math
}  // namespace stan
#endif
