#ifndef STAN_MATH_PRIM_SCAL_PROB_NEG_BINOMIAL_RNG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_NEG_BINOMIAL_RNG_HPP

#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/err/check_less.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/meta/max_size.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <boost/random/gamma_distribution.hpp>
#include <boost/random/poisson_distribution.hpp>
#include <boost/random/variate_generator.hpp>

namespace stan {
namespace math {

/**
 * Return a negative binomial random variate with the specified shape and
 * inverse scale parameters using the given random number generator.
 *
 * alpha and beta can each be a scalar or a one-dimensional container. Any
 * non-scalar inputs must be the same size.
 *
 * @tparam T_shape Type of shape parameter
 * @tparam T_inv Type of inverse scale parameter
 * @tparam RNG type of random number generator
 * @param alpha (Sequence of) positive shape parameter(s)
 * @param beta (Sequence of) positive inverse scale parameter(s)
 * @param rng random number generator
 * @return (Sequence of) negative binomial random variate(s)
 * @throw std::domain_error if alpha or beta are nonpositive
 * @throw std::invalid_argument if non-scalar arguments are of different
 * sizes
 */
template <typename T_shape, typename T_inv, class RNG>
inline typename VectorBuilder<true, int, T_shape, T_inv>::type neg_binomial_rng(
    const T_shape& alpha, const T_inv& beta, RNG& rng) {
  using boost::gamma_distribution;
  using boost::random::poisson_distribution;
  using boost::variate_generator;

  static const char* function = "neg_binomial_rng";

  // gamma_rng params must be positive and finite
  check_positive_finite(function, "Shape parameter", alpha);
  check_positive_finite(function, "Inverse scale parameter", beta);
  check_consistent_sizes(function, "Shape parameter", alpha,
                         "Inverse scale Parameter", beta);

  scalar_seq_view<T_shape> alpha_vec(alpha);
  scalar_seq_view<T_inv> beta_vec(beta);
  size_t N = max_size(alpha, beta);
  VectorBuilder<true, int, T_shape, T_inv> output(N);

  for (size_t n = 0; n < N; ++n) {
    double rng_from_gamma = variate_generator<RNG&, gamma_distribution<> >(
        rng, gamma_distribution<>(alpha_vec[n], 1.0 / beta_vec[n]))();

    // same as the constraints for poisson_rng
    check_less(function, "Random number that came from gamma distribution",
               rng_from_gamma, POISSON_MAX_RATE);
    check_not_nan(function, "Random number that came from gamma distribution",
                  rng_from_gamma);
    check_nonnegative(function,
                      "Random number that came from gamma distribution",
                      rng_from_gamma);

    output[n] = variate_generator<RNG&, poisson_distribution<> >(
        rng, poisson_distribution<>(rng_from_gamma))();
  }

  return output.data();
}

}  // namespace math
}  // namespace stan
#endif
