#ifndef STAN_MATH_PRIM_SCAL_FUN_BESSEL_SECOND_KIND_HPP
#define STAN_MATH_PRIM_SCAL_FUN_BESSEL_SECOND_KIND_HPP

#include <boost/math/special_functions/bessel.hpp>

namespace stan {
namespace math {

/**
 *
   \f[
   \mbox{bessel\_second\_kind}(v, x) =
   \begin{cases}
     \textrm{error} & \mbox{if } x \leq 0 \\
     Y_v(x) & \mbox{if } x > 0 \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{bessel\_second\_kind}(v, x)}{\partial x} =
   \begin{cases}
     \textrm{error} & \mbox{if } x \leq 0 \\
     \frac{\partial\, Y_v(x)}{\partial x} & \mbox{if } x > 0 \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   Y_v(x)=\frac{J_v(x)\cos(v\pi)-J_{-v}(x)}{\sin(v\pi)}
   \f]

   \f[
   \frac{\partial \, Y_v(x)}{\partial x} = \frac{v}{x}Y_v(x)-Y_{v+1}(x)
   \f]
 *
 */
template <typename T2>
inline T2 bessel_second_kind(int v, const T2 z) {
  return boost::math::cyl_neumann(v, z);
}

}  // namespace math
}  // namespace stan

#endif
