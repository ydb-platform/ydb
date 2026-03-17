#ifndef STAN_MATH_PRIM_SCAL_PROB_BINOMIAL_LOGIT_LPMF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BINOMIAL_LOGIT_LPMF_HPP

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
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/fun/inc_beta.hpp>
#include <boost/random/binomial_distribution.hpp>
#include <boost/random/variate_generator.hpp>

namespace stan {
namespace math {

/**
 * Binomial log PMF in logit parametrization. Binomial(n|n, inv_logit(alpha))
 *
 * If given vectors of matching lengths, returns
 * the log sum of probabilities.
 *
 * @param n successes variable
 * @param N population size parameter
 * @param alpha logit transformed probability parameter
 *
 * @return log probability or log sum of probabilities
 *
 * @throw std::domain_error if N is negative or probability parameter is invalid
 * @throw std::invalid_argument if vector sizes do not match
 */
template <bool propto, typename T_n, typename T_N, typename T_prob>
typename return_type<T_prob>::type binomial_logit_lpmf(const T_n& n,
                                                       const T_N& N,
                                                       const T_prob& alpha) {
  typedef typename stan::partials_return_type<T_n, T_N, T_prob>::type
      T_partials_return;

  static const char* function = "binomial_logit_lpmf";

  if (size_zero(n, N, alpha))
    return 0.0;

  T_partials_return logp = 0;
  check_bounded(function, "Successes variable", n, 0, N);
  check_nonnegative(function, "Population size parameter", N);
  check_finite(function, "Probability parameter", alpha);
  check_consistent_sizes(function, "Successes variable", n,
                         "Population size parameter", N,
                         "Probability parameter", alpha);

  if (!include_summand<propto, T_prob>::value)
    return 0.0;

  scalar_seq_view<T_n> n_vec(n);
  scalar_seq_view<T_N> N_vec(N);
  scalar_seq_view<T_prob> alpha_vec(alpha);
  size_t size = max_size(n, N, alpha);

  operands_and_partials<T_prob> ops_partials(alpha);

  if (include_summand<propto>::value) {
    for (size_t i = 0; i < size; ++i)
      logp += binomial_coefficient_log(N_vec[i], n_vec[i]);
  }

  VectorBuilder<true, T_partials_return, T_prob> log_inv_logit_alpha(
      length(alpha));
  for (size_t i = 0; i < length(alpha); ++i)
    log_inv_logit_alpha[i] = log_inv_logit(value_of(alpha_vec[i]));

  VectorBuilder<true, T_partials_return, T_prob> log_inv_logit_neg_alpha(
      length(alpha));
  for (size_t i = 0; i < length(alpha); ++i)
    log_inv_logit_neg_alpha[i] = log_inv_logit(-value_of(alpha_vec[i]));

  for (size_t i = 0; i < size; ++i)
    logp += n_vec[i] * log_inv_logit_alpha[i]
            + (N_vec[i] - n_vec[i]) * log_inv_logit_neg_alpha[i];

  if (length(alpha) == 1) {
    T_partials_return temp1 = 0;
    T_partials_return temp2 = 0;
    for (size_t i = 0; i < size; ++i) {
      temp1 += n_vec[i];
      temp2 += N_vec[i] - n_vec[i];
    }
    if (!is_constant_struct<T_prob>::value) {
      ops_partials.edge1_.partials_[0]
          += temp1 * inv_logit(-value_of(alpha_vec[0]))
             - temp2 * inv_logit(value_of(alpha_vec[0]));
    }
  } else {
    if (!is_constant_struct<T_prob>::value) {
      for (size_t i = 0; i < size; ++i)
        ops_partials.edge1_.partials_[i]
            += n_vec[i] * inv_logit(-value_of(alpha_vec[i]))
               - (N_vec[i] - n_vec[i]) * inv_logit(value_of(alpha_vec[i]));
    }
  }

  return ops_partials.build(logp);
}

template <typename T_n, typename T_N, typename T_prob>
inline typename return_type<T_prob>::type binomial_logit_lpmf(
    const T_n& n, const T_N& N, const T_prob& alpha) {
  return binomial_logit_lpmf<false>(n, N, alpha);
}

}  // namespace math
}  // namespace stan
#endif
