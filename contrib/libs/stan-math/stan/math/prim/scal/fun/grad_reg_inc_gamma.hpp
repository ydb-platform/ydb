#ifndef STAN_MATH_PRIM_SCAL_FUN_GRAD_REG_INC_GAMMA_HPP
#define STAN_MATH_PRIM_SCAL_FUN_GRAD_REG_INC_GAMMA_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/scal/fun/gamma_p.hpp>
#include <stan/math/prim/scal/fun/gamma_q.hpp>
#include <stan/math/prim/scal/fun/is_inf.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <cmath>
#include <limits>

namespace stan {
namespace math {

/**
 * Gradient of the regularized incomplete gamma functions igamma(a, z)
 *
 * For small z, the gradient is computed via the series expansion;
 * for large z, the series is numerically inaccurate due to cancellation
 * and the asymptotic expansion is used.
 *
 * @param a   shape parameter, a > 0
 * @param z   location z >= 0
 * @param g   stan::math::tgamma(a) (precomputed value)
 * @param dig boost::math::digamma(a) (precomputed value)
 * @param precision required precision; applies to series expansion only
 * @param max_steps number of steps to take.
 * @throw throws std::domain_error if not converged after max_steps
 *   or increment overflows to inf.
 *
 * For the asymptotic expansion, the gradient is given by:
   \f[
   \begin{array}{rcl}
   \Gamma(a, z) & = & z^{a-1}e^{-z} \sum_{k=0}^N \frac{(a-1)_k}{z^k} \qquad , z
 \gg a\\
   Q(a, z) & = & \frac{z^{a-1}e^{-z}}{\Gamma(a)} \sum_{k=0}^N
 \frac{(a-1)_k}{z^k}\\
   (a)_k & = & (a)_{k-1}(a-k)\\
   \frac{d}{da} (a)_k & = & (a)_{k-1} + (a-k)\frac{d}{da} (a)_{k-1}\\
   \frac{d}{da}Q(a, z) & = & (log(z) - \psi(a)) Q(a, z)\\
   && + \frac{z^{a-1}e^{-z}}{\Gamma(a)} \sum_{k=0}^N \left(\frac{d}{da}
 (a-1)_k\right) \frac{1}{z^k} \end{array} \f]
 */
template <typename T1, typename T2>
typename return_type<T1, T2>::type grad_reg_inc_gamma(T1 a, T2 z, T1 g, T1 dig,
                                                      double precision = 1e-6,
                                                      int max_steps = 1e5) {
  using std::exp;
  using std::fabs;
  using std::log;
  typedef typename return_type<T1, T2>::type TP;

  if (is_nan(a) || is_nan(z) || is_nan(g) || is_nan(dig))
    return std::numeric_limits<TP>::quiet_NaN();

  T2 l = log(z);
  if (z >= a && z >= 8) {
    // asymptotic expansion http://dlmf.nist.gov/8.11#E2
    TP S = 0;
    T1 a_minus_one_minus_k = a - 1;
    T1 fac = a_minus_one_minus_k;  // falling_factorial(a-1, k)
    T1 dfac = 1;                   // d/da[falling_factorial(a-1, k)]
    T2 zpow = z;                   // z ** k
    TP delta = dfac / zpow;

    for (int k = 1; k < 10; ++k) {
      a_minus_one_minus_k -= 1;

      S += delta;

      zpow *= z;
      dfac = a_minus_one_minus_k * dfac + fac;
      fac *= a_minus_one_minus_k;
      delta = dfac / zpow;

      if (is_inf(delta))
        domain_error("grad_reg_inc_gamma", "is not converging", "", "");
    }

    return gamma_q(a, z) * (l - dig) + exp(-z + (a - 1) * l) * S / g;
  } else {
    // gradient of series expansion http://dlmf.nist.gov/8.7#E3
    TP S = 0;
    TP log_s = 0.0;
    double s_sign = 1.0;
    T2 log_z = log(z);
    TP log_delta = log_s - 2 * log(a);
    for (int k = 1; k <= max_steps; ++k) {
      S += s_sign >= 0.0 ? exp(log_delta) : -exp(log_delta);
      log_s += log_z - log(k);
      s_sign = -s_sign;
      log_delta = log_s - 2 * log(k + a);
      if (is_inf(log_delta))
        domain_error("grad_reg_inc_gamma", "is not converging", "", "");
      if (log_delta <= log(precision))
        return gamma_p(a, z) * (dig - l) + exp(a * l) * S / g;
    }
    domain_error("grad_reg_inc_gamma", "k (internal counter)", max_steps,
                 "exceeded ",
                 " iterations, gamma function gradient did not converge.");
    return std::numeric_limits<TP>::infinity();
  }
}

}  // namespace math
}  // namespace stan
#endif
