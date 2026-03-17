#ifndef STAN_MATH_PRIM_SCAL_FUN_BESSEL_FIRST_KIND_HPP
#define STAN_MATH_PRIM_SCAL_FUN_BESSEL_FIRST_KIND_HPP

#include <boost/math/special_functions/bessel.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>

namespace stan {
namespace math {

/**
 *
   \f[
   \mbox{bessel\_first\_kind}(v, x) =
   \begin{cases}
     J_v(x) & \mbox{if } -\infty\leq x \leq \infty \\[6pt]
     \textrm{error} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{bessel\_first\_kind}(v, x)}{\partial x} =
   \begin{cases}
     \frac{\partial\, J_v(x)}{\partial x} & \mbox{if } -\infty\leq x\leq \infty
 \\[6pt] \textrm{error} & \mbox{if } x = \textrm{NaN} \end{cases} \f]

   \f[
   J_v(x)=\left(\frac{1}{2}x\right)^v
   \sum_{k=0}^\infty \frac{\left(-\frac{1}{4}x^2\right)^k}{k!\, \Gamma(v+k+1)}
   \f]

   \f[
   \frac{\partial \, J_v(x)}{\partial x} = \frac{v}{x}J_v(x)-J_{v+1}(x)
   \f]
 *
 */
template <typename T2>
inline T2 bessel_first_kind(int v, const T2 z) {
  check_not_nan("bessel_first_kind", "z", z);
  return boost::math::cyl_bessel_j(v, z);
}

}  // namespace math
}  // namespace stan
#endif
