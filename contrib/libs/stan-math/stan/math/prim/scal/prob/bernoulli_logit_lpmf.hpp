#ifndef STAN_MATH_PRIM_SCAL_PROB_BERNOULLI_LOGIT_LPMF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BERNOULLI_LOGIT_LPMF_HPP

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
 * Returns the log PMF of the logit-parametrized Bernoulli distribution. If
 * containers are supplied, returns the log sum of the probabilities.
 *
 * @tparam T_n type of integer parameter
 * @tparam T_prob type of chance of success parameter
 * @param n integer parameter
 * @param theta logit-transformed chance of success parameter
 * @return log probability or log sum of probabilities
 * @throw std::domain_error if theta is infinite.
 * @throw std::invalid_argument if container sizes mismatch.
 */
template <bool propto, typename T_n, typename T_prob>
typename return_type<T_prob>::type bernoulli_logit_lpmf(const T_n& n,
                                                        const T_prob& theta) {
  static const char* function = "bernoulli_logit_lpmf";
  typedef
      typename stan::partials_return_type<T_n, T_prob>::type T_partials_return;

  using std::exp;

  if (size_zero(n, theta))
    return 0.0;

  T_partials_return logp(0.0);

  check_bounded(function, "n", n, 0, 1);
  check_not_nan(function, "Logit transformed probability parameter", theta);
  check_consistent_sizes(function, "Random variable", n,
                         "Probability parameter", theta);

  if (!include_summand<propto, T_prob>::value)
    return 0.0;

  scalar_seq_view<T_n> n_vec(n);
  scalar_seq_view<T_prob> theta_vec(theta);
  size_t N = max_size(n, theta);
  operands_and_partials<T_prob> ops_partials(theta);

  for (size_t n = 0; n < N; n++) {
    const T_partials_return theta_dbl = value_of(theta_vec[n]);

    const int sign = 2 * n_vec[n] - 1;
    const T_partials_return ntheta = sign * theta_dbl;
    const T_partials_return exp_m_ntheta = exp(-ntheta);

    // Handle extreme values gracefully using Taylor approximations.
    static const double cutoff = 20.0;
    if (ntheta > cutoff)
      logp -= exp_m_ntheta;
    else if (ntheta < -cutoff)
      logp += ntheta;
    else
      logp -= log1p(exp_m_ntheta);

    if (!is_constant_struct<T_prob>::value) {
      if (ntheta > cutoff)
        ops_partials.edge1_.partials_[n] -= exp_m_ntheta;
      else if (ntheta < -cutoff)
        ops_partials.edge1_.partials_[n] += sign;
      else
        ops_partials.edge1_.partials_[n]
            += sign * exp_m_ntheta / (exp_m_ntheta + 1);
    }
  }
  return ops_partials.build(logp);
}

template <typename T_n, typename T_prob>
inline typename return_type<T_prob>::type bernoulli_logit_lpmf(
    const T_n& n, const T_prob& theta) {
  return bernoulli_logit_lpmf<false>(n, theta);
}

}  // namespace math
}  // namespace stan
#endif
