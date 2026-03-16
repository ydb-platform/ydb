#ifndef STAN_MATH_PRIM_SCAL_FUN_LOG_RISING_FACTORIAL_HPP
#define STAN_MATH_PRIM_SCAL_FUN_LOG_RISING_FACTORIAL_HPP

#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <limits>

namespace stan {
namespace math {

/**
 * Return the natural logarithm of the rising factorial from the
 * first specified argument to the second.
 *
   \f[
   \mbox{log\_rising\_factorial}(x, n) =
   \begin{cases}
     \textrm{error} & \mbox{if } x \leq 0\\
     \ln x^{(n)} & \mbox{if } x > 0 \textrm{ and } -\infty \leq n \leq \infty
 \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN or } n = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{log\_rising\_factorial}(x, n)}{\partial x} =
   \begin{cases}
     \textrm{error} & \mbox{if } x \leq 0\\
     \Psi(x+n) - \Psi(x) & \mbox{if } x > 0 \textrm{ and } -\infty \leq n \leq
 \infty \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN or } n = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{log\_rising\_factorial}(x, n)}{\partial n} =
   \begin{cases}
     \textrm{error} & \mbox{if } x \leq 0\\
     \Psi(x+n) & \mbox{if } x > 0 \textrm{ and } -\infty \leq n \leq \infty
 \\[6pt] \textrm{NaN} & \mbox{if } x = \textrm{NaN or } n = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @tparam T1 type of first argument x
 * @tparam T2 type of second argument n
 * @param[in] x first argument
 * @param[in] n second argument
 * @return natural logarithm of the rising factorial from x to n
 * @throw std::domain_error if the first argument is not positive
 */
template <typename T1, typename T2>
inline typename return_type<T1, T2>::type log_rising_factorial(const T1& x,
                                                               const T2& n) {
  if (is_nan(x) || is_nan(n))
    return std::numeric_limits<double>::quiet_NaN();
  static const char* function = "log_rising_factorial";
  check_positive(function, "first argument", x);
  return lgamma(x + n) - lgamma(x);
}

}  // namespace math
}  // namespace stan

#endif
