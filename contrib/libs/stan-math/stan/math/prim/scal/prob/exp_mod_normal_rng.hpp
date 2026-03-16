#ifndef STAN_MATH_PRIM_SCAL_PROB_EXP_MOD_NORMAL_RNG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_EXP_MOD_NORMAL_RNG_HPP

#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/meta/max_size.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/prob/exponential_rng.hpp>
#include <stan/math/prim/scal/prob/normal_rng.hpp>
#include <boost/random/normal_distribution.hpp>
#include <boost/random/variate_generator.hpp>

namespace stan {
namespace math {

/**
 * Return an exponentially modified normal random variate for the
 * given location, scale, and inverse scale using the specified random
 * number generator.
 *
 * mu, sigma, and lambda can each be a scalar or a one-dimensional container.
 * Any non-scalar inputs must be the same size.
 *
 * @tparam T_loc Type of location parameter
 * @tparam T_scale Type of scale parameter
 * @tparam T_inv_scale Type of inverse scale parameter
 * @tparam RNG type of random number generator
 * @param mu (Sequence of) location parameter(s)
 * @param sigma (Sequence of) scale parameter(s)
 * @param lambda (Sequence of) inverse scale parameter(s)
 * @param rng random number generator
 * @return (Sequence of) Exponentially modified normal random variate(s)
 * @throw std::domain_error if mu is infinite, sigma is nonpositive,
 * or lambda is nonpositive
 * @throw std::invalid_argument if non-scalar arguments are of different
 * sizes
 */
template <typename T_loc, typename T_scale, typename T_inv_scale, class RNG>
inline typename VectorBuilder<true, double, T_loc, T_scale, T_inv_scale>::type
exp_mod_normal_rng(const T_loc& mu, const T_scale& sigma,
                   const T_inv_scale& lambda, RNG& rng) {
  static const char* function = "exp_mod_normal_rng";

  check_finite(function, "Location parameter", mu);
  check_positive_finite(function, "Scale parameter", sigma);
  check_positive_finite(function, "Inv_scale parameter", lambda);
  check_consistent_sizes(function, "Location parameter", mu, "Scale Parameter",
                         sigma, "Inv_scale Parameter", lambda);

  scalar_seq_view<T_loc> mu_vec(mu);
  scalar_seq_view<T_scale> sigma_vec(sigma);
  scalar_seq_view<T_inv_scale> lambda_vec(lambda);
  size_t N = max_size(mu, sigma, lambda);
  VectorBuilder<true, double, T_loc, T_scale, T_inv_scale> output(N);

  for (size_t n = 0; n < N; ++n)
    output[n] = normal_rng(mu_vec[n], sigma_vec[n], rng)
                + exponential_rng(lambda_vec[n], rng);

  return output.data();
}

}  // namespace math
}  // namespace stan
#endif
