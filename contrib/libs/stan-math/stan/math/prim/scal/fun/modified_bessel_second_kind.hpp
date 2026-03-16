#ifndef STAN_MATH_PRIM_SCAL_FUN_MODIFIED_BESSEL_SECOND_KIND_HPP
#define STAN_MATH_PRIM_SCAL_FUN_MODIFIED_BESSEL_SECOND_KIND_HPP

#include <boost/math/special_functions/bessel.hpp>

namespace stan {
namespace math {

/**
 *
   \f[
   \mbox{modified\_bessel\_second\_kind}(v, z) =
   \begin{cases}
     \textrm{error} & \mbox{if } z \leq 0 \\
     K_v(z) & \mbox{if } z > 0 \\[6pt]
     \textrm{NaN} & \mbox{if } z = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{modified\_bessel\_second\_kind}(v, z)}{\partial z} =
   \begin{cases}
     \textrm{error} & \mbox{if } z \leq 0 \\
     \frac{\partial\, K_v(z)}{\partial z} & \mbox{if } z > 0 \\[6pt]
     \textrm{NaN} & \mbox{if } z = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   {K_v}(z)
   =
   \frac{\pi}{2}\cdot\frac{I_{-v}(z) - I_{v}(z)}{\sin(v\pi)}
   \f]

   \f[
   \frac{\partial \, K_v(z)}{\partial z} = -\frac{v}{z}K_v(z)-K_{v-1}(z)
   \f]
 *
 */
template <typename T2>
inline T2 modified_bessel_second_kind(int v, const T2 z) {
  return boost::math::cyl_bessel_k(v, z);
}

}  // namespace math
}  // namespace stan

#endif
