#ifndef STAN_MATH_PRIM_SCAL_PROB_BERNOULLI_LPMF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BERNOULLI_LPMF_HPP

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
#include <cmath>

namespace stan {
namespace math {

/**
 * Returns the log PMF of the Bernoulli distribution. If containers are
 * supplied, returns the log sum of the probabilities.
 *
 * @tparam T_n type of integer parameters
 * @tparam T_prob type of chance of success parameters
 * @param n integer parameter
 * @param theta chance of success parameter
 * @return log probability or log sum of probabilities
 * @throw std::domain_error if theta is not a valid probability
 * @throw std::invalid_argument if container sizes mismatch.
 */
template <bool propto, typename T_n, typename T_prob>
typename return_type<T_prob>::type bernoulli_lpmf(const T_n& n,
                                                  const T_prob& theta) {
  static const char* function = "bernoulli_lpmf";
  typedef
      typename stan::partials_return_type<T_n, T_prob>::type T_partials_return;

  using std::log;

  if (size_zero(n, theta))
    return 0.0;

  T_partials_return logp(0.0);

  check_bounded(function, "n", n, 0, 1);
  check_finite(function, "Probability parameter", theta);
  check_bounded(function, "Probability parameter", theta, 0.0, 1.0);
  check_consistent_sizes(function, "Random variable", n,
                         "Probability parameter", theta);

  if (!include_summand<propto, T_prob>::value)
    return 0.0;

  scalar_seq_view<T_n> n_vec(n);
  scalar_seq_view<T_prob> theta_vec(theta);
  size_t N = max_size(n, theta);
  operands_and_partials<T_prob> ops_partials(theta);

  if (length(theta) == 1) {
    size_t sum = 0;
    for (size_t n = 0; n < N; n++) {
      sum += value_of(n_vec[n]);
    }
    const T_partials_return theta_dbl = value_of(theta_vec[0]);
    // avoid nans when sum == N or sum == 0
    if (sum == N) {
      logp += N * log(theta_dbl);
      if (!is_constant_struct<T_prob>::value)
        ops_partials.edge1_.partials_[0] += N / theta_dbl;
    } else if (sum == 0) {
      logp += N * log1m(theta_dbl);
      if (!is_constant_struct<T_prob>::value)
        ops_partials.edge1_.partials_[0] += N / (theta_dbl - 1);
    } else {
      const T_partials_return log_theta = log(theta_dbl);
      const T_partials_return log1m_theta = log1m(theta_dbl);

      logp += sum * log_theta;
      logp += (N - sum) * log1m_theta;

      if (!is_constant_struct<T_prob>::value) {
        ops_partials.edge1_.partials_[0] += sum / theta_dbl;
        ops_partials.edge1_.partials_[0] += (N - sum) / (theta_dbl - 1);
      }
    }
  } else {
    for (size_t n = 0; n < N; n++) {
      const int n_int = value_of(n_vec[n]);
      const T_partials_return theta_dbl = value_of(theta_vec[n]);

      if (n_int == 1)
        logp += log(theta_dbl);
      else
        logp += log1m(theta_dbl);

      if (!is_constant_struct<T_prob>::value) {
        if (n_int == 1)
          ops_partials.edge1_.partials_[n] += 1.0 / theta_dbl;
        else
          ops_partials.edge1_.partials_[n] += 1.0 / (theta_dbl - 1);
      }
    }
  }
  return ops_partials.build(logp);
}

template <typename T_y, typename T_prob>
inline typename return_type<T_prob>::type bernoulli_lpmf(const T_y& n,
                                                         const T_prob& theta) {
  return bernoulli_lpmf<false>(n, theta);
}

}  // namespace math
}  // namespace stan
#endif
