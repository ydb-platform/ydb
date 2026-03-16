#ifndef STAN_MATH_PRIM_SCAL_FUN_LOG_FALLING_FACTORIAL_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LOG_FALLING_FACTORIAL_HPP

#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <limits>

namespace stan {
namespace math {

/**
 *
 * Return the natural log of the falling factorial of the
 * specified arguments.
 *

   \f[
   \mbox{log\_falling\_factorial}(x, n) =
   \begin{cases}
     \textrm{error} & \mbox{if } x \leq 0\\
     \ln (x)_n & \mbox{if } x > 0 \textrm{ and } -\infty \leq n \leq \infty
 \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN or } n = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{log\_falling\_factorial}(x, n)}{\partial x} =
   \begin{cases}
     \textrm{error} & \mbox{if } x \leq 0\\
     \Psi(x) & \mbox{if } x > 0 \textrm{ and } -\infty \leq n \leq \infty
 \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN or } n = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{log\_falling\_factorial}(x, n)}{\partial n} =
   \begin{cases}
     \textrm{error} & \mbox{if } x \leq 0\\
     -\Psi(n) & \mbox{if } x > 0 \textrm{ and } -\infty \leq n \leq \infty
 \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN or } n = \textrm{NaN}
   \end{cases}
   \f]

 * @tparam T1 type of first argument
 * @tparam T2 type of second argument
 * @param[in] x First argument
 * @param[in] n Second argument
 * @return log of falling factorial of arguments
 * @throw std::domain_error if the first argument is not
 * positive
 */
template <typename T1, typename T2>
inline typename return_type<T1, T2>::type log_falling_factorial(const T1 x,
                                                                const T2 n) {
  if (is_nan(x) || is_nan(n))
    return std::numeric_limits<double>::quiet_NaN();
  static const char* function = "log_falling_factorial";
  check_positive(function, "first argument", x);
  return lgamma(x + 1) - lgamma(x - n + 1);
}

}  // namespace math
}  // namespace stan
#endif
