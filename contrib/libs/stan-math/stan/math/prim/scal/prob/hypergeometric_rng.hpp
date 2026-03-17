#ifndef STAN_MATH_PRIM_SCAL_PROB_HYPERGEOMETRIC_RNG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_HYPERGEOMETRIC_RNG_HPP

#include <boost/math/distributions/hypergeometric.hpp>

#include <stan/math/prim/scal/err/check_bounded.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/prob/uniform_rng.hpp>

namespace stan {
namespace math {

template <class RNG>
inline int hypergeometric_rng(int N, int a, int b, RNG& rng) {
  using boost::math::hypergeometric_distribution;
  using boost::variate_generator;

  static const char* function = "hypergeometric_rng";

  check_bounded(function, "Draws parameter", N, 0, a + b);
  check_positive(function, "Draws parameter", N);
  check_positive(function, "Successes in population parameter", a);
  check_positive(function, "Failures in population parameter", b);

  hypergeometric_distribution<> dist(b, N, a + b);

  double u = uniform_rng(0.0, 1.0, rng);
  int min = 0;
  int max = a - 1;
  while (min < max) {
    int mid = (min + max) / 2;
    if (cdf(dist, mid + 1) > u)
      max = mid;
    else
      min = mid + 1;
  }
  return min + 1;
}

}  // namespace math
}  // namespace stan
#endif
