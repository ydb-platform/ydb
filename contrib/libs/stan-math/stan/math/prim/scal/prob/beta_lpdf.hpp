#ifndef STAN_MATH_PRIM_SCAL_PROB_BETA_LPDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BETA_LPDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_less_or_equal.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/log1m.hpp>
#include <stan/math/prim/scal/fun/multiply_log.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>
#include <stan/math/prim/scal/fun/lbeta.hpp>
#include <stan/math/prim/scal/meta/contains_nonconstant_struct.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/fun/grad_reg_inc_beta.hpp>
#include <stan/math/prim/scal/fun/inc_beta.hpp>
#include <boost/math/special_functions/gamma.hpp>
#include <boost/random/gamma_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * The log of the beta density for the specified scalar(s) given the specified
 * sample size(s). y, alpha, or beta can each either be scalar or a vector.
 * Any vector inputs must be the same length.
 *
 * <p> The result log probability is defined to be the sum of
 * the log probabilities for each observation/alpha/beta triple.
 *
 * Prior sample sizes, alpha and beta, must be greater than 0.
 *
 * @param y (Sequence of) scalar(s).
 * @param alpha (Sequence of) prior sample size(s).
 * @param beta (Sequence of) prior sample size(s).
 * @return The log of the product of densities.
 * @tparam T_y Type of scalar outcome.
 * @tparam T_scale_succ Type of prior scale for successes.
 * @tparam T_scale_fail Type of prior scale for failures.
 */
template <bool propto, typename T_y, typename T_scale_succ,
          typename T_scale_fail>
typename return_type<T_y, T_scale_succ, T_scale_fail>::type beta_lpdf(
    const T_y& y, const T_scale_succ& alpha, const T_scale_fail& beta) {
  static const char* function = "beta_lpdf";

  typedef
      typename stan::partials_return_type<T_y, T_scale_succ, T_scale_fail>::type
          T_partials_return;

  using std::log;

  if (size_zero(y, alpha, beta))
    return 0.0;

  T_partials_return logp(0.0);

  check_positive_finite(function, "First shape parameter", alpha);
  check_positive_finite(function, "Second shape parameter", beta);
  check_not_nan(function, "Random variable", y);
  check_consistent_sizes(function, "Random variable", y,
                         "First shape parameter", alpha,
                         "Second shape parameter", beta);
  check_nonnegative(function, "Random variable", y);
  check_less_or_equal(function, "Random variable", y, 1);

  if (!include_summand<propto, T_y, T_scale_succ, T_scale_fail>::value)
    return 0.0;

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_scale_succ> alpha_vec(alpha);
  scalar_seq_view<T_scale_fail> beta_vec(beta);
  size_t N = max_size(y, alpha, beta);

  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_dbl = value_of(y_vec[n]);
    if (y_dbl < 0 || y_dbl > 1)
      return LOG_ZERO;
  }

  operands_and_partials<T_y, T_scale_succ, T_scale_fail> ops_partials(y, alpha,
                                                                      beta);

  VectorBuilder<include_summand<propto, T_y, T_scale_succ>::value,
                T_partials_return, T_y>
      log_y(length(y));
  VectorBuilder<include_summand<propto, T_y, T_scale_fail>::value,
                T_partials_return, T_y>
      log1m_y(length(y));

  for (size_t n = 0; n < length(y); n++) {
    if (include_summand<propto, T_y, T_scale_succ>::value)
      log_y[n] = log(value_of(y_vec[n]));
    if (include_summand<propto, T_y, T_scale_fail>::value)
      log1m_y[n] = log1m(value_of(y_vec[n]));
  }

  VectorBuilder<include_summand<propto, T_scale_succ>::value, T_partials_return,
                T_scale_succ>
      lgamma_alpha(length(alpha));
  VectorBuilder<!is_constant_struct<T_scale_succ>::value, T_partials_return,
                T_scale_succ>
      digamma_alpha(length(alpha));
  for (size_t n = 0; n < length(alpha); n++) {
    if (include_summand<propto, T_scale_succ>::value)
      lgamma_alpha[n] = lgamma(value_of(alpha_vec[n]));
    if (!is_constant_struct<T_scale_succ>::value)
      digamma_alpha[n] = digamma(value_of(alpha_vec[n]));
  }

  VectorBuilder<include_summand<propto, T_scale_fail>::value, T_partials_return,
                T_scale_fail>
      lgamma_beta(length(beta));
  VectorBuilder<!is_constant_struct<T_scale_fail>::value, T_partials_return,
                T_scale_fail>
      digamma_beta(length(beta));

  for (size_t n = 0; n < length(beta); n++) {
    if (include_summand<propto, T_scale_fail>::value)
      lgamma_beta[n] = lgamma(value_of(beta_vec[n]));
    if (!is_constant_struct<T_scale_fail>::value)
      digamma_beta[n] = digamma(value_of(beta_vec[n]));
  }

  VectorBuilder<include_summand<propto, T_scale_succ, T_scale_fail>::value,
                T_partials_return, T_scale_succ, T_scale_fail>
      lgamma_alpha_beta(max_size(alpha, beta));

  VectorBuilder<contains_nonconstant_struct<T_scale_succ, T_scale_fail>::value,
                T_partials_return, T_scale_succ, T_scale_fail>
      digamma_alpha_beta(max_size(alpha, beta));

  for (size_t n = 0; n < max_size(alpha, beta); n++) {
    const T_partials_return alpha_beta
        = value_of(alpha_vec[n]) + value_of(beta_vec[n]);
    if (include_summand<propto, T_scale_succ, T_scale_fail>::value)
      lgamma_alpha_beta[n] = lgamma(alpha_beta);
    if (contains_nonconstant_struct<T_scale_succ, T_scale_fail>::value)
      digamma_alpha_beta[n] = digamma(alpha_beta);
  }

  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return alpha_dbl = value_of(alpha_vec[n]);
    const T_partials_return beta_dbl = value_of(beta_vec[n]);

    if (include_summand<propto, T_scale_succ, T_scale_fail>::value)
      logp += lgamma_alpha_beta[n];
    if (include_summand<propto, T_scale_succ>::value)
      logp -= lgamma_alpha[n];
    if (include_summand<propto, T_scale_fail>::value)
      logp -= lgamma_beta[n];
    if (include_summand<propto, T_y, T_scale_succ>::value)
      logp += (alpha_dbl - 1.0) * log_y[n];
    if (include_summand<propto, T_y, T_scale_fail>::value)
      logp += (beta_dbl - 1.0) * log1m_y[n];

    if (!is_constant_struct<T_y>::value)
      ops_partials.edge1_.partials_[n]
          += (alpha_dbl - 1) / y_dbl + (beta_dbl - 1) / (y_dbl - 1);
    if (!is_constant_struct<T_scale_succ>::value)
      ops_partials.edge2_.partials_[n]
          += log_y[n] + digamma_alpha_beta[n] - digamma_alpha[n];
    if (!is_constant_struct<T_scale_fail>::value)
      ops_partials.edge3_.partials_[n]
          += log1m_y[n] + digamma_alpha_beta[n] - digamma_beta[n];
  }
  return ops_partials.build(logp);
}

template <typename T_y, typename T_scale_succ, typename T_scale_fail>
inline typename return_type<T_y, T_scale_succ, T_scale_fail>::type beta_lpdf(
    const T_y& y, const T_scale_succ& alpha, const T_scale_fail& beta) {
  return beta_lpdf<false>(y, alpha, beta);
}

}  // namespace math
}  // namespace stan
#endif
