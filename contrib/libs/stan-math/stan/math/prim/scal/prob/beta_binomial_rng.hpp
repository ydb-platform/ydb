#ifndef STAN_MATH_PRIM_SCAL_PROB_BETA_BINOMIAL_RNG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BETA_BINOMIAL_RNG_HPP

#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/prob/binomial_rng.hpp>
#include <stan/math/prim/scal/prob/beta_rng.hpp>

namespace stan {
namespace math {

/**
 * Return a beta-binomial random variate with the specified population size,
 * success, and failure parameters using the given random number generator.
 *
 * N, alpha, and beta can each be a scalar or a one-dimensional container. Any
 * non-scalar inputs must be the same size.
 *
 * @tparam T_N Type of population size parameter
 * @tparam T_shape1 Type of success parameter
 * @tparam T_shape2 Type of failure parameter
 * @tparam RNG type of random number generator
 * @param N (Sequence of) population size parameter(s)
 * @param alpha (Sequence of) positive success parameter(s)
 * @param beta (Sequence of) positive failure parameter(s)
 * @param rng random number generator
 * @return (Sequence of) beta-binomial random variate(s)
 * @throw std::domain_error if N is negative, or alpha or beta are nonpositive
 * @throw std::invalid_argument if non-scalar arguments are of different
 * sizes
 */
template <typename T_N, typename T_shape1, typename T_shape2, class RNG>
inline typename VectorBuilder<true, int, T_N, T_shape1, T_shape2>::type
beta_binomial_rng(const T_N &N, const T_shape1 &alpha, const T_shape2 &beta,
                  RNG &rng) {
  static const char *function = "beta_binomial_rng";

  check_nonnegative(function, "Population size parameter", N);
  check_positive_finite(function, "First prior sample size parameter", alpha);
  check_positive_finite(function, "Second prior sample size parameter", beta);
  check_consistent_sizes(function, "First prior sample size parameter", alpha,
                         "Second prior sample size parameter", beta);

  auto p = beta_rng(alpha, beta, rng);
  return binomial_rng(N, p, rng);
}

}  // namespace math
}  // namespace stan
#endif
