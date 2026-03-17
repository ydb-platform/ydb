#ifndef STAN_MATH_PRIM_SCAL_FUN_FALLING_FACTORIAL_HPP
#define STAN_MATH_PRIM_SCAL_FUN_FALLING_FACTORIAL_HPP

#include <boost/math/special_functions/factorials.hpp>
#include <stan/math/prim/scal/fun/boost_policy.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <limits>

namespace stan {
namespace math {

/**
 * Return the falling factorial function evaluated
 * at the inputs.
 * Will throw for NaN x and for negative n
 *
 * @tparam T Type of x argument.
 * @param x Argument.
 * @param n Argument
 * @return Result of falling factorial function.
 * @throw std::domain_error if x is NaN
 * @throw std::domain_error if n is negative
 *
   \f[
   \mbox{falling\_factorial}(x, n) =
   \begin{cases}
     \textrm{error} & \mbox{if } x \leq 0\\
     (x)_n & \mbox{if } x > 0 \textrm{ and } -\infty \leq n \leq \infty \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN or } n = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{falling\_factorial}(x, n)}{\partial x} =
   \begin{cases}
     \textrm{error} & \mbox{if } x \leq 0\\
     \frac{\partial\, (x)_n}{\partial x} & \mbox{if } x > 0 \textrm{ and }
 -\infty \leq n \leq \infty \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN or
 } n = \textrm{NaN} \end{cases} \f]

   \f[
   \frac{\partial\, \mbox{falling\_factorial}(x, n)}{\partial n} =
   \begin{cases}
     \textrm{error} & \mbox{if } x \leq 0\\
     \frac{\partial\, (x)_n}{\partial n} & \mbox{if } x > 0 \textrm{ and }
 -\infty \leq n \leq \infty \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN or
 } n = \textrm{NaN} \end{cases} \f]

   \f[
   (x)_n=\frac{\Gamma(x+1)}{\Gamma(x-n+1)}
   \f]

   \f[
   \frac{\partial \, (x)_n}{\partial x} = (x)_n\Psi(x+1)
   \f]

   \f[
   \frac{\partial \, (x)_n}{\partial n} = -(x)_n\Psi(n+1)
   \f]
 *
 */
template <typename T>
inline typename boost::math::tools::promote_args<T>::type falling_factorial(
    const T& x, int n) {
  static const char* function = "falling_factorial";
  check_not_nan(function, "first argument", x);
  check_nonnegative(function, "second argument", n);
  return boost::math::falling_factorial(x, n, boost_policy_t());
}

}  // namespace math
}  // namespace stan

#endif
