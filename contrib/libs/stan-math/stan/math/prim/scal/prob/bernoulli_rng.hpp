#ifndef STAN_MATH_PRIM_SCAL_PROB_BERNOULLI_RNG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BERNOULLI_RNG_HPP

#include <stan/math/prim/scal/err/check_bounded.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <boost/random/bernoulli_distribution.hpp>
#include <boost/random/variate_generator.hpp>

namespace stan {
namespace math {

/**
 * Return a Bernoulli random variate with specified chance of success
 * parameter using the specified random number generator.
 *
 * theta can be a scalar or a one-dimensional container.
 *
 * @tparam T_theta type of chance of success parameter
 * @tparam RNG type of random number generator
 * @param theta (Sequence of) chance of success parameter(s)
 * @param rng random number generator
 * @return (Sequence of) Bernoulli random variate(s)
 * @throw std::domain_error if chance of success parameter is less than zero or
 * greater than one.
 */
template <typename T_theta, class RNG>
inline typename VectorBuilder<true, int, T_theta>::type bernoulli_rng(
    const T_theta& theta, RNG& rng) {
  using boost::bernoulli_distribution;
  using boost::variate_generator;

  static const char* function = "bernoulli_rng";

  check_finite(function, "Probability parameter", theta);
  check_bounded(function, "Probability parameter", theta, 0.0, 1.0);

  scalar_seq_view<T_theta> theta_vec(theta);
  size_t N = length(theta);
  VectorBuilder<true, int, T_theta> output(N);

  for (size_t n = 0; n < N; ++n) {
    variate_generator<RNG&, bernoulli_distribution<> > bernoulli_rng(
        rng, bernoulli_distribution<>(theta_vec[n]));
    output[n] = bernoulli_rng();
  }

  return output.data();
}

}  // namespace math
}  // namespace stan
#endif
