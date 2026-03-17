#ifndef STAN_MATH_PRIM_MAT_PROB_MULTINOMIAL_RNG_HPP
#define STAN_MATH_PRIM_MAT_PROB_MULTINOMIAL_RNG_HPP

#include <stan/math/prim/mat/err/check_simplex.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/fun/multiply_log.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/prob/binomial_rng.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <boost/math/special_functions/gamma.hpp>
#include <boost/random/uniform_01.hpp>
#include <boost/random/variate_generator.hpp>
#include <vector>

namespace stan {
namespace math {

template <class RNG>
inline std::vector<int> multinomial_rng(
    const Eigen::Matrix<double, Eigen::Dynamic, 1>& theta, int N, RNG& rng) {
  static const char* function = "multinomial_rng";

  check_simplex(function, "Probabilities parameter", theta);
  check_positive(function, "number of trials variables", N);

  std::vector<int> result(theta.size(), 0);
  double mass_left = 1.0;
  int n_left = N;
  for (int k = 0; n_left > 0 && k < theta.size(); ++k) {
    double p = theta[k] / mass_left;
    if (p > 1.0)
      p = 1.0;
    result[k] = binomial_rng(n_left, p, rng);
    n_left -= result[k];
    mass_left -= theta[k];
  }
  return result;
}

}  // namespace math
}  // namespace stan
#endif
