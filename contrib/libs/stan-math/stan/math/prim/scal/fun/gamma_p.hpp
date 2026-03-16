#ifndef STAN_MATH_PRIM_SCAL_FUN_GAMMA_P_HPP
#define STAN_MATH_PRIM_SCAL_FUN_GAMMA_P_HPP

#include <boost/math/special_functions/gamma.hpp>

namespace stan {
namespace math {

/**
 *
   \f[
   \mbox{gamma\_p}(a, z) =
   \begin{cases}
     \textrm{error} & \mbox{if } a\leq 0 \textrm{ or } z < 0\\
     P(a, z) & \mbox{if } a > 0, z \geq 0 \\[6pt]
     \textrm{NaN} & \mbox{if } a = \textrm{NaN or } z = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{gamma\_p}(a, z)}{\partial a} =
   \begin{cases}
     \textrm{error} & \mbox{if } a\leq 0 \textrm{ or } z < 0\\
     \frac{\partial\, P(a, z)}{\partial a} & \mbox{if } a > 0, z \geq 0 \\[6pt]
     \textrm{NaN} & \mbox{if } a = \textrm{NaN or } z = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{gamma\_p}(a, z)}{\partial z} =
   \begin{cases}
     \textrm{error} & \mbox{if } a\leq 0 \textrm{ or } z < 0\\
     \frac{\partial\, P(a, z)}{\partial z} & \mbox{if } a > 0, z \geq 0 \\[6pt]
     \textrm{NaN} & \mbox{if } a = \textrm{NaN or } z = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   P(a, z)=\frac{1}{\Gamma(a)}\int_0^zt^{a-1}e^{-t}dt
   \f]

   \f[
   \frac{\partial \, P(a, z)}{\partial a} =
 -\frac{\Psi(a)}{\Gamma^2(a)}\int_0^zt^{a-1}e^{-t}dt
   + \frac{1}{\Gamma(a)}\int_0^z (a-1)t^{a-2}e^{-t}dt
   \f]

   \f[
   \frac{\partial \, P(a, z)}{\partial z} = \frac{z^{a-1}e^{-z}}{\Gamma(a)}
   \f]
   * @throws domain_error if x is at pole

 */
inline double gamma_p(double x, double a) { return boost::math::gamma_p(x, a); }

}  // namespace math
}  // namespace stan
#endif
