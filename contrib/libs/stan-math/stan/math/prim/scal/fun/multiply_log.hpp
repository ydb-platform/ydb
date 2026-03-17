#ifndef STAN_MATH_PRIM_SCAL_FUN_MULTIPLY_LOG_HPP
#define STAN_MATH_PRIM_SCAL_FUN_MULTIPLY_LOG_HPP

#include <boost/math/tools/promotion.hpp>

namespace stan {
namespace math {

/**
 * Calculated the value of the first argument
 * times log of the second argument while behaving
 * properly with 0 inputs.
 *
 * \f$ a * \log b \f$.
 *
 *
   \f[
   \mbox{multiply\_log}(x, y) =
   \begin{cases}
     0 & \mbox{if } x=y=0\\
     x\ln y & \mbox{if } x, y\neq0 \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{multiply\_log}(x, y)}{\partial x} =
   \begin{cases}
     \infty & \mbox{if } x=y=0\\
     \ln y & \mbox{if } x, y\neq 0 \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{multiply\_log}(x, y)}{\partial y} =
   \begin{cases}
     \infty & \mbox{if } x=y=0\\
     \frac{x}{y} & \mbox{if } x, y\neq 0 \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param a the first variable
 * @param b the second variable
 *
 * @return a * log(b)
 */
template <typename T_a, typename T_b>
inline typename boost::math::tools::promote_args<T_a, T_b>::type multiply_log(
    const T_a a, const T_b b) {
  using std::log;
  if (b == 0.0 && a == 0.0)
    return 0.0;
  return a * log(b);
}

}  // namespace math
}  // namespace stan

#endif
