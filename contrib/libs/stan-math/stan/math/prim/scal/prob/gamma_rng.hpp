#ifndef STAN_MATH_PRIM_SCAL_PROB_GAMMA_RNG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_GAMMA_RNG_HPP

#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/meta/max_size.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <boost/random/gamma_distribution.hpp>
#include <boost/random/variate_generator.hpp>

namespace stan {
namespace math {

/**
 * Return a gamma random variate for the given shape and inverse
 * scale parameters using the specified random number generator.
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
 * @return (Sequence of) gamma random variate(s)
 * @throw std::domain_error if alpha or beta are nonpositive
 * @throw std::invalid_argument if non-scalar arguments are of different
 * sizes
 */
template <typename T_shape, typename T_inv, class RNG>
inline typename VectorBuilder<true, double, T_shape, T_inv>::type gamma_rng(
    const T_shape& alpha, const T_inv& beta, RNG& rng) {
  using boost::gamma_distribution;
  using boost::variate_generator;

  static const char* function = "gamma_rng";

  check_positive_finite(function, "Shape parameter", alpha);
  check_positive_finite(function, "Inverse scale parameter", beta);
  check_consistent_sizes(function, "Shape parameter", alpha,
                         "Inverse scale Parameter", beta);

  scalar_seq_view<T_shape> alpha_vec(alpha);
  scalar_seq_view<T_inv> beta_vec(beta);
  size_t N = max_size(alpha, beta);
  VectorBuilder<true, double, T_shape, T_inv> output(N);

  for (size_t n = 0; n < N; ++n) {
    // Convert rate (inverse scale) argument to scale for boost
    variate_generator<RNG&, gamma_distribution<> > gamma_rng(
        rng, gamma_distribution<>(alpha_vec[n],
                                  1 / static_cast<double>(beta_vec[n])));
    output[n] = gamma_rng();
  }

  return output.data();
}

}  // namespace math
}  // namespace stan
#endif
