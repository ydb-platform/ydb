#ifndef STAN_MATH_PRIM_SCAL_FUN_INC_BETA_DDA_HPP
#define STAN_MATH_PRIM_SCAL_FUN_INC_BETA_DDA_HPP

#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/scal/fun/inc_beta.hpp>
#include <stan/math/prim/scal/fun/inc_beta_ddb.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Returns the partial derivative of the regularized
 * incomplete beta function, I_{z}(a, b) with respect to a.
 * The power series used to compute the deriative tends to
 * converge slowly when a and b are large, especially if z
 * approaches 1.  The implementation will throw an exception
 * if the series have not converged within 100,000 iterations.
 * The current implementation has been tested for values
 * of a and b up to 12500 and z = 0.999.
 *
 * @tparam T scalar types of arguments
 * @param a a
 * @param b b
 * @param z upper bound of the integral
 * @param digamma_a value of digamma(a)
 * @param digamma_ab value of digamma(b)
 * @return partial derivative of the incomplete beta with respect to a
 *
 * @pre a >= 0
 * @pre b >= 0
 * @pre 0 <= z <= 1
 */
template <typename T>
T inc_beta_dda(T a, T b, T z, T digamma_a, T digamma_ab) {
  using std::log;

  if (b > a)
    if ((0.1 < z && z <= 0.75 && b > 500) || (0.01 < z && z <= 0.1 && b > 2500)
        || (0.001 < z && z <= 0.01 && b > 1e5))
      return -inc_beta_ddb(b, a, 1 - z, digamma_a, digamma_ab);

  if (z > 0.75 && a < 500)
    return -inc_beta_ddb(b, a, 1 - z, digamma_a, digamma_ab);
  if (z > 0.9 && a < 2500)
    return -inc_beta_ddb(b, a, 1 - z, digamma_a, digamma_ab);
  if (z > 0.99 && a < 1e5)
    return -inc_beta_ddb(b, a, 1 - z, digamma_a, digamma_ab);
  if (z > 0.999)
    return -inc_beta_ddb(b, a, 1 - z, digamma_a, digamma_ab);

  double threshold = 1e-10;

  digamma_a += 1.0 / a;  // Need digamma(a + 1), not digamma(a);

  // Common prefactor to regularize numerator and denomentator
  T prefactor = (a + 1) / (a + b);
  prefactor = prefactor * prefactor * prefactor;

  T sum_numer = (digamma_ab - digamma_a) * prefactor;
  T sum_denom = prefactor;

  T summand = prefactor * z * (a + b) / (a + 1);

  T k = 1;
  digamma_ab += 1.0 / (a + b);
  digamma_a += 1.0 / (a + 1);

  while (fabs(summand) > threshold) {
    sum_numer += (digamma_ab - digamma_a) * summand;
    sum_denom += summand;

    summand *= (1 + (a + b) / k) * (1 + k) / (1 + (a + 1) / k);
    digamma_ab += 1.0 / (a + b + k);
    digamma_a += 1.0 / (a + 1 + k);
    ++k;
    summand *= z / k;

    if (k > 1e5)
      domain_error("inc_beta_dda", "did not converge within 10000 iterations",
                   "", "");
  }
  return inc_beta(a, b, z) * (log(z) + sum_numer / sum_denom);
}

}  // namespace math
}  // namespace stan
#endif
