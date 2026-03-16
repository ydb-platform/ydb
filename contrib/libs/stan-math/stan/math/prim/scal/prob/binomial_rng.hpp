#ifndef STAN_MATH_PRIM_SCAL_PROB_BINOMIAL_RNG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BINOMIAL_RNG_HPP

#include <stan/math/prim/scal/err/check_bounded.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/max_size.hpp>
#include <boost/random/binomial_distribution.hpp>
#include <boost/random/variate_generator.hpp>

namespace stan {
namespace math {

/**
 * Return a pseudorandom binomial random variable for the given population
 * size and chance of success parameters using the specified random number
 * generator.
 *
 * beta can be a scalar or a one-dimensional container.
 *
 * @tparam T_N Type of population size parameter
 * @tparam T_theta Type of change of success parameter
 * @tparam RNG class of rng
 * @param N (Sequence of) population size parameter(s)
 * @param theta (Sequence of) chance of success parameter(s)
 * @param rng random number generator
 * @return (Sequence of) binomial random variate(s)
 * @throw std::domain_error if N is negative
 * @throw std::domain_error if theta is not a valid probability
 */
template <typename T_N, typename T_theta, class RNG>
inline typename VectorBuilder<true, int, T_N, T_theta>::type binomial_rng(
    const T_N& N, const T_theta& theta, RNG& rng) {
  using boost::binomial_distribution;
  using boost::variate_generator;

  static const char* function = "binomial_rng";

  check_nonnegative(function, "Population size parameter", N);
  check_bounded(function, "Probability parameter", theta, 0.0, 1.0);
  check_consistent_sizes(function, "Population size parameter", N,
                         "Probability Parameter", theta);

  scalar_seq_view<T_N> N_vec(N);
  scalar_seq_view<T_theta> theta_vec(theta);
  size_t M = max_size(N, theta);
  VectorBuilder<true, int, T_N, T_theta> output(M);

  for (size_t m = 0; m < M; ++m) {
    variate_generator<RNG&, binomial_distribution<> > binomial_rng(
        rng, binomial_distribution<>(N_vec[m], theta_vec[m]));

    output[m] = binomial_rng();
  }

  return output.data();
}

}  // namespace math
}  // namespace stan
#endif
