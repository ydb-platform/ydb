#ifndef STAN_MATH_PRIM_SCAL_PROB_BERNOULLI_CDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BERNOULLI_CDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_bounded.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/inv_logit.hpp>
#include <stan/math/prim/scal/fun/log1m.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <boost/random/bernoulli_distribution.hpp>
#include <boost/random/variate_generator.hpp>

namespace stan {
namespace math {

/**
 * Returns the CDF of the Bernoulli distribution. If containers are
 * supplied, returns the product of the probabilities.
 *
 * @tparam T_n type of integer parameter
 * @tparam T_prob type of chance of success parameter
 * @param n integer parameter
 * @param theta chance of success parameter
 * @return probability or product of probabilities
 * @throw std::domain_error if theta is not a valid probability
 * @throw std::invalid_argument if container sizes mismatch.
 */
template <typename T_n, typename T_prob>
typename return_type<T_prob>::type bernoulli_cdf(const T_n& n,
                                                 const T_prob& theta) {
  static const char* function = "bernoulli_cdf";
  typedef
      typename stan::partials_return_type<T_n, T_prob>::type T_partials_return;

  if (size_zero(n, theta))
    return 1.0;

  T_partials_return P(1.0);

  check_finite(function, "Probability parameter", theta);
  check_bounded(function, "Probability parameter", theta, 0.0, 1.0);
  check_consistent_sizes(function, "Random variable", n,
                         "Probability parameter", theta);

  scalar_seq_view<T_n> n_vec(n);
  scalar_seq_view<T_prob> theta_vec(theta);
  size_t size = max_size(n, theta);

  operands_and_partials<T_prob> ops_partials(theta);

  // Explicit return for extreme values
  // The gradients are technically ill-defined, but treated as zero
  for (size_t i = 0; i < stan::length(n); i++) {
    if (value_of(n_vec[i]) < 0)
      return ops_partials.build(0.0);
  }

  for (size_t i = 0; i < size; i++) {
    // Explicit results for extreme values
    // The gradients are technically ill-defined, but treated as zero
    if (value_of(n_vec[i]) >= 1)
      continue;

    const T_partials_return Pi = 1 - value_of(theta_vec[i]);

    P *= Pi;

    if (!is_constant_struct<T_prob>::value)
      ops_partials.edge1_.partials_[i] += -1 / Pi;
  }

  if (!is_constant_struct<T_prob>::value) {
    for (size_t i = 0; i < stan::length(theta); ++i)
      ops_partials.edge1_.partials_[i] *= P;
  }
  return ops_partials.build(P);
}

}  // namespace math
}  // namespace stan
#endif
