#ifndef STAN_MATH_PRIM_SCAL_PROB_PARETO_TYPE_2_RNG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_PARETO_TYPE_2_RNG_HPP

#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/meta/max_size.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/prob/exponential_rng.hpp>
#include <stan/math/prim/scal/prob/normal_rng.hpp>
#include <boost/random/uniform_real_distribution.hpp>
#include <boost/random/variate_generator.hpp>

namespace stan {
namespace math {

/**
 * Return a Pareto type 2 random variate for the given location,
 * scale, and shape using the specified random number generator.
 *
 * mu, lambda, and alpha can each be a scalar or a one-dimensional container.
 * Any non-scalar inputs must be the same size.
 *
 * @tparam T_loc Type of location parameter
 * @tparam T_scale Type of scale parameter
 * @tparam T_shape Type of shape parameter
 * @tparam RNG type of random number generator
 * @param mu (Sequence of) location parameter(s)
 * @param lambda (Sequence of) scale parameter(s)
 * @param alpha (Sequence of) shape parameter(s)
 * @param rng random number generator
 * @return (Sequence of) Pareto type 2 random variate(s)
 * @throw std::domain_error if mu is infinite or lambda or alpha are
 * nonpositive,
 * @throw std::invalid_argument if non-scalar arguments are of different
 * sizes
 */
template <typename T_loc, typename T_scale, typename T_shape, class RNG>
inline typename VectorBuilder<true, double, T_loc, T_scale, T_shape>::type
pareto_type_2_rng(const T_loc& mu, const T_scale& lambda, const T_shape& alpha,
                  RNG& rng) {
  using boost::random::uniform_real_distribution;
  using boost::variate_generator;
  static const char* function = "pareto_type_2_rng";

  check_finite(function, "Location parameter", mu);
  check_positive_finite(function, "Scale parameter", lambda);
  check_positive_finite(function, "Shape parameter", alpha);
  check_consistent_sizes(function, "Location parameter", mu, "Scale Parameter",
                         lambda, "Shape Parameter", alpha);

  scalar_seq_view<T_loc> mu_vec(mu);
  scalar_seq_view<T_scale> lambda_vec(lambda);
  scalar_seq_view<T_shape> alpha_vec(alpha);
  size_t N = max_size(mu, lambda, alpha);
  VectorBuilder<true, double, T_loc, T_scale, T_shape> output(N);

  variate_generator<RNG&, uniform_real_distribution<> > uniform_rng(
      rng, uniform_real_distribution<>(0.0, 1.0));
  for (size_t n = 0; n < N; ++n)
    output[n] = (std::pow(1.0 - uniform_rng(), -1.0 / alpha_vec[n]) - 1.0)
                    * lambda_vec[n]
                + mu_vec[n];

  return output.data();
}

}  // namespace math
}  // namespace stan
#endif
