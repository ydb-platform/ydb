#ifndef STAN_MATH_PRIM_SCAL_PROB_PARETO_RNG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_PARETO_RNG_HPP

#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/meta/max_size.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <boost/random/exponential_distribution.hpp>
#include <boost/random/variate_generator.hpp>

namespace stan {
namespace math {

/**
 * Return a Pareto random variate for the given shape and scale
 * parameters using the specified random number generator.
 *
 * y_min and alpha can each be a scalar or a one-dimensional container. Any
 * non-scalar inputs must be the same size.
 *
 * @tparam T_scale Type of scale parameter
 * @tparam T_shape Type of shape parameter
 * @tparam RNG type of random number generator
 * @param y_min (Sequence of) positive scale parameter(s)
 * @param alpha (Sequence of) positive shape parameter(s)
 * @param rng random number generator
 * @return (Sequence of) Pareto random variate(s)
 * @throw std::domain_error if y_min or alpha are nonpositive
 * @throw std::invalid_argument if non-scalar arguments are of different
 * sizes
 */
template <typename T_shape, typename T_scale, class RNG>
inline typename VectorBuilder<true, double, T_shape, T_scale>::type pareto_rng(
    const T_scale& y_min, const T_shape& alpha, RNG& rng) {
  using boost::exponential_distribution;
  using boost::variate_generator;

  static const char* function = "pareto_rng";

  check_positive_finite(function, "Scale parameter", y_min);
  check_positive_finite(function, "Shape parameter", alpha);
  check_consistent_sizes(function, "Scale Parameter", y_min, "Shape parameter",
                         alpha);

  scalar_seq_view<T_scale> y_min_vec(y_min);
  scalar_seq_view<T_shape> alpha_vec(alpha);
  size_t N = max_size(y_min, alpha);
  VectorBuilder<true, double, T_scale, T_shape> output(N);

  for (size_t n = 0; n < N; ++n) {
    variate_generator<RNG&, exponential_distribution<> > exp_rng(
        rng, exponential_distribution<>(alpha_vec[n]));
    output[n] = y_min_vec[n] * std::exp(exp_rng());
  }

  return output.data();
}

}  // namespace math
}  // namespace stan
#endif
