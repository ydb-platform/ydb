#ifndef STAN_MATH_PRIM_SCAL_PROB_BINOMIAL_LCDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BINOMIAL_LCDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_bounded.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_greater_or_equal.hpp>
#include <stan/math/prim/scal/err/check_less_or_equal.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/inv_logit.hpp>
#include <stan/math/prim/scal/fun/log1m.hpp>
#include <stan/math/prim/scal/fun/log_inv_logit.hpp>
#include <stan/math/prim/scal/fun/multiply_log.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/fun/binomial_coefficient_log.hpp>
#include <stan/math/prim/scal/fun/lbeta.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/fun/inc_beta.hpp>
#include <boost/random/binomial_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Returns the log CDF for the binomial distribution evaluated at the
 * specified success, population size, and chance of success. If given
 * containers of matching lengths, returns the log sum of probabilities.
 *
 * @tparam T_n type of successes parameter
 * @tparam T_N type of population size parameter
 * @tparam theta type of chance of success parameter
 * @param n successes parameter
 * @param N population size parameter
 * @param theta chance of success parameter
 * @return log probability or log sum of probabilities
 * @throw std::domain_error if N is negative
 * @throw std::domain_error if theta is not a valid probability
 * @throw std::invalid_argument if container sizes mismatch
 */
template <typename T_n, typename T_N, typename T_prob>
typename return_type<T_prob>::type binomial_lcdf(const T_n& n, const T_N& N,
                                                 const T_prob& theta) {
  static const char* function = "binomial_lcdf";
  typedef typename stan::partials_return_type<T_n, T_N, T_prob>::type
      T_partials_return;

  if (size_zero(n, N, theta))
    return 0.0;

  T_partials_return P(0.0);

  check_nonnegative(function, "Population size parameter", N);
  check_finite(function, "Probability parameter", theta);
  check_bounded(function, "Probability parameter", theta, 0.0, 1.0);
  check_consistent_sizes(function, "Successes variable", n,
                         "Population size parameter", N,
                         "Probability parameter", theta);

  scalar_seq_view<T_n> n_vec(n);
  scalar_seq_view<T_N> N_vec(N);
  scalar_seq_view<T_prob> theta_vec(theta);
  size_t size = max_size(n, N, theta);

  using std::exp;
  using std::exp;
  using std::log;
  using std::pow;

  operands_and_partials<T_prob> ops_partials(theta);

  // Explicit return for extreme values
  // The gradients are technically ill-defined,
  // but treated as negative infinity
  for (size_t i = 0; i < stan::length(n); i++) {
    if (value_of(n_vec[i]) < 0)
      return ops_partials.build(negative_infinity());
  }

  for (size_t i = 0; i < size; i++) {
    // Explicit results for extreme values
    // The gradients are technically ill-defined, but treated as zero
    if (value_of(n_vec[i]) >= value_of(N_vec[i])) {
      continue;
    }
    const T_partials_return n_dbl = value_of(n_vec[i]);
    const T_partials_return N_dbl = value_of(N_vec[i]);
    const T_partials_return theta_dbl = value_of(theta_vec[i]);
    const T_partials_return betafunc = exp(lbeta(N_dbl - n_dbl, n_dbl + 1));
    const T_partials_return Pi
        = inc_beta(N_dbl - n_dbl, n_dbl + 1, 1 - theta_dbl);

    P += log(Pi);

    if (!is_constant_struct<T_prob>::value)
      ops_partials.edge1_.partials_[i]
          -= pow(theta_dbl, n_dbl) * pow(1 - theta_dbl, N_dbl - n_dbl - 1)
             / betafunc / Pi;
  }
  return ops_partials.build(P);
}

}  // namespace math
}  // namespace stan
#endif
