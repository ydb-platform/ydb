#ifndef STAN_MATH_PRIM_SCAL_FUN_MODIFIED_BESSEL_FIRST_KIND_HPP
#define STAN_MATH_PRIM_SCAL_FUN_MODIFIED_BESSEL_FIRST_KIND_HPP

#include <boost/math/special_functions/bessel.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>

namespace stan {
namespace math {

/**
 *
   \f[
   \mbox{modified\_bessel\_first\_kind}(v, z) =
   \begin{cases}
     I_v(z) & \mbox{if } -\infty\leq z \leq \infty \\[6pt]
     \textrm{error} & \mbox{if } z = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{modified\_bessel\_first\_kind}(v, z)}{\partial z} =
   \begin{cases}
     \frac{\partial\, I_v(z)}{\partial z} & \mbox{if } -\infty\leq z\leq \infty
 \\[6pt] \textrm{error} & \mbox{if } z = \textrm{NaN} \end{cases} \f]

   \f[
     {I_v}(z) = \left(\frac{1}{2}z\right)^v\sum_{k=0}^\infty
 \frac{\left(\frac{1}{4}z^2\right)^k}{k!\Gamma(v+k+1)} \f]

     \f[
     \frac{\partial \, I_v(z)}{\partial z} = I_{v-1}(z)-\frac{v}{z}I_v(z)
     \f]
 *
 */
template <typename T2>
inline T2 modified_bessel_first_kind(int v, const T2 z) {
  check_not_nan("modified_bessel_first_kind", "z", z);

  return boost::math::cyl_bessel_i(v, z);
}

}  // namespace math
}  // namespace stan

#endif
