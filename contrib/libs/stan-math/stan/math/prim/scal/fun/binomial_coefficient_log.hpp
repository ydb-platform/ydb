#ifndef STAN_MATH_PRIM_SCAL_FUN_BINOMIAL_COEFFICIENT_LOG_HPP
#define STAN_MATH_PRIM_SCAL_FUN_BINOMIAL_COEFFICIENT_LOG_HPP

#include <stan/math/prim/scal/fun/lgamma.hpp>
#include <boost/math/tools/promotion.hpp>

namespace stan {
namespace math {
/**
 * Return the log of the binomial coefficient for the specified
 * arguments.
 *
 * The binomial coefficient, \f${N \choose n}\f$, read "N choose n", is
 * defined for \f$0 \leq n \leq N\f$ by
 *
 * \f${N \choose n} = \frac{N!}{n! (N-n)!}\f$.
 *
 * This function uses Gamma functions to define the log
 * and generalize the arguments to continuous N and n.
 *
 * \f$ \log {N \choose n}
 * = \log \ \Gamma(N+1) - \log \Gamma(n+1) - \log \Gamma(N-n+1)\f$.
 *
   \f[
   \mbox{binomial\_coefficient\_log}(x, y) =
   \begin{cases}
     \textrm{error} & \mbox{if } y > x \textrm{ or } y < 0\\
     \ln\Gamma(x+1) & \mbox{if } 0\leq y \leq x \\
     \quad -\ln\Gamma(y+1)& \\
     \quad -\ln\Gamma(x-y+1)& \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{binomial\_coefficient\_log}(x, y)}{\partial x} =
   \begin{cases}
     \textrm{error} & \mbox{if } y > x \textrm{ or } y < 0\\
     \Psi(x+1) & \mbox{if } 0\leq y \leq x \\
     \quad -\Psi(x-y+1)& \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]

   \f[
   \frac{\partial\, \mbox{binomial\_coefficient\_log}(x, y)}{\partial y} =
   \begin{cases}
     \textrm{error} & \mbox{if } y > x \textrm{ or } y < 0\\
     -\Psi(y+1) & \mbox{if } 0\leq y \leq x \\
     \quad +\Psi(x-y+1)& \\[6pt]
     \textrm{NaN} & \mbox{if } x = \textrm{NaN or } y = \textrm{NaN}
   \end{cases}
   \f]
 *
 * @param N total number of objects.
 * @param n number of objects chosen.
 * @return log (N choose n).
 */
template <typename T_N, typename T_n>
inline typename boost::math::tools::promote_args<T_N, T_n>::type
binomial_coefficient_log(const T_N N, const T_n n) {
  using std::log;
  const double CUTOFF = 1000;
  if (N - n < CUTOFF) {
    T_N N_plus_1 = N + 1;
    return lgamma(N_plus_1) - lgamma(n + 1) - lgamma(N_plus_1 - n);
  } else {
    typename boost::math::tools::promote_args<T_N, T_n>::type N_minus_n = N - n;
    double one_twelfth = 1.0 / 12;
    return n * log(N_minus_n) + (N + 0.5) * log(N / N_minus_n) + one_twelfth / N
           - n - one_twelfth / N_minus_n - lgamma(n + 1);
  }
}

}  // namespace math
}  // namespace stan
#endif
