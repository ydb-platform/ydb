#ifndef STAN_MATH_PRIM_SCAL_FUN_RISING_FACTORIAL_HPP
#define STAN_MATH_PRIM_SCAL_FUN_RISING_FACTORIAL_HPP

#include <boost/math/special_functions/factorials.hpp>
#include <stan/math/prim/scal/fun/boost_policy.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <limits>

namespace stan {
namespace math {

/**
 * Return the rising factorial function evaluated
 * at the inputs.
 * Will throw for NaN x and for negative n
 *
 * @tparam T Type of x argument.
 * @param x Argument.
 * @param n Argument
 * @return Result of rising factorial function.
 * @throw std::domain_error if x is NaN
 * @throw std::domain_error if n is negative
 *
 \f[
 \mbox{rising\_factorial}(x, n) =
 \begin{cases}
 \textrm{error} & \mbox{if } x \leq 0\\
 x^{(n)} & \mbox{if } x > 0 \textrm{ and } -\infty \leq n \leq \infty \\[6pt]
 \textrm{NaN} & \mbox{if } x = \textrm{NaN or } n = \textrm{NaN}
 \end{cases}
 \f]

 \f[
 \frac{\partial\, \mbox{rising\_factorial}(x, n)}{\partial x} =
 \begin{cases}
 \textrm{error} & \mbox{if } x \leq 0\\
 \frac{\partial\, x^{(n)}}{\partial x} & \mbox{if } x > 0 \textrm{ and } -\infty
 \leq n \leq \infty \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN or } n =
 \textrm{NaN} \end{cases} \f]

 \f[
 \frac{\partial\, \mbox{rising\_factorial}(x, n)}{\partial n} =
 \begin{cases}
 \textrm{error} & \mbox{if } x \leq 0\\
 \frac{\partial\, x^{(n)}}{\partial n} & \mbox{if } x > 0 \textrm{ and } -\infty
 \leq n \leq \infty \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN or } n =
 \textrm{NaN} \end{cases} \f]

 \f[
 x^{(n)}=\frac{\Gamma(x+n)}{\Gamma(x)}
 \f]

 \f[
 \frac{\partial \, x^{(n)}}{\partial x} = x^{(n)}(\Psi(x+n)-\Psi(x))
 \f]

 \f[
 \frac{\partial \, x^{(n)}}{\partial n} = (x)_n\Psi(x+n)
 \f]
 *
 */
template <typename T>
inline typename boost::math::tools::promote_args<T>::type rising_factorial(
    const T& x, int n) {
  static const char* function = "rising_factorial";
  check_not_nan(function, "first argument", x);
  check_nonnegative(function, "second argument", n);
  return boost::math::rising_factorial(x, n, boost_policy_t());
}
}  // namespace math
}  // namespace stan
#endif
