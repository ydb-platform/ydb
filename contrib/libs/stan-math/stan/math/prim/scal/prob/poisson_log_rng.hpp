#ifndef STAN_MATH_PRIM_SCAL_PROB_POISSON_LOG_RNG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_POISSON_LOG_RNG_HPP

#include <stan/math/prim/scal/err/check_less.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <boost/random/poisson_distribution.hpp>
#include <boost/random/variate_generator.hpp>

namespace stan {
namespace math {

/**
 * Return a Poisson random variate with specified log rate parameter
 * using the given random number generator.
 *
 * lambda can be a scalar or a one-dimensional container.
 *
 * @tparam T_rate type of log rate parameter
 * @tparam RNG type of random number generator
 * @param alpha (Sequence of) log rate parameter(s)
 * @param rng random number generator
 * @return (Sequence of) Poisson random variate(s)
 * @throw std::domain_error if alpha is nonfinite
 */
template <typename T_rate, class RNG>
inline typename VectorBuilder<true, int, T_rate>::type poisson_log_rng(
    const T_rate& alpha, RNG& rng) {
  using boost::random::poisson_distribution;
  using boost::variate_generator;

  static const char* function = "poisson_log_rng";
  static const double POISSON_MAX_LOG_RATE = 30 * std::log(2);

  check_finite(function, "Log rate parameter", alpha);
  check_less(function, "Log rate parameter", alpha, POISSON_MAX_LOG_RATE);

  scalar_seq_view<T_rate> alpha_vec(alpha);
  size_t N = length(alpha);
  VectorBuilder<true, int, T_rate> output(N);

  for (size_t n = 0; n < N; ++n) {
    variate_generator<RNG&, poisson_distribution<> > poisson_rng(
        rng, poisson_distribution<>(std::exp(alpha_vec[n])));
    output[n] = poisson_rng();
  }

  return output.data();
}

}  // namespace math
}  // namespace stan
#endif
