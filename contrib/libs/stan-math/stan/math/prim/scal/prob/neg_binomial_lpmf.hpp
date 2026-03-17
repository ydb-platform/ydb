#ifndef STAN_MATH_PRIM_SCAL_PROB_NEG_BINOMIAL_LPMF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_NEG_BINOMIAL_LPMF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/fun/binomial_coefficient_log.hpp>
#include <stan/math/prim/scal/fun/multiply_log.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>
#include <stan/math/prim/scal/fun/lbeta.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/fun/grad_reg_inc_beta.hpp>
#include <stan/math/prim/scal/fun/inc_beta.hpp>
#include <stan/math/prim/scal/meta/max_size.hpp>
#include <boost/math/special_functions/digamma.hpp>
#include <boost/random/negative_binomial_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>

namespace stan {
namespace math {

// NegBinomial(n|alpha, beta)  [alpha > 0;  beta > 0;  n >= 0]
template <bool propto, typename T_n, typename T_shape, typename T_inv_scale>
typename return_type<T_shape, T_inv_scale>::type neg_binomial_lpmf(
    const T_n& n, const T_shape& alpha, const T_inv_scale& beta) {
  typedef typename stan::partials_return_type<T_n, T_shape, T_inv_scale>::type
      T_partials_return;

  static const char* function = "neg_binomial_lpmf";

  if (size_zero(n, alpha, beta))
    return 0.0;

  T_partials_return logp(0.0);
  check_nonnegative(function, "Failures variable", n);
  check_positive_finite(function, "Shape parameter", alpha);
  check_positive_finite(function, "Inverse scale parameter", beta);
  check_consistent_sizes(function, "Failures variable", n, "Shape parameter",
                         alpha, "Inverse scale parameter", beta);

  if (!include_summand<propto, T_shape, T_inv_scale>::value)
    return 0.0;

  using std::log;
  using std::log;

  scalar_seq_view<T_n> n_vec(n);
  scalar_seq_view<T_shape> alpha_vec(alpha);
  scalar_seq_view<T_inv_scale> beta_vec(beta);
  size_t size = max_size(n, alpha, beta);

  operands_and_partials<T_shape, T_inv_scale> ops_partials(alpha, beta);

  size_t len_ab = max_size(alpha, beta);
  VectorBuilder<true, T_partials_return, T_shape, T_inv_scale> lambda(len_ab);
  for (size_t i = 0; i < len_ab; ++i)
    lambda[i] = value_of(alpha_vec[i]) / value_of(beta_vec[i]);

  VectorBuilder<true, T_partials_return, T_inv_scale> log1p_beta(length(beta));
  for (size_t i = 0; i < length(beta); ++i)
    log1p_beta[i] = log1p(value_of(beta_vec[i]));

  VectorBuilder<true, T_partials_return, T_inv_scale> log_beta_m_log1p_beta(
      length(beta));
  for (size_t i = 0; i < length(beta); ++i)
    log_beta_m_log1p_beta[i] = log(value_of(beta_vec[i])) - log1p_beta[i];

  VectorBuilder<true, T_partials_return, T_inv_scale, T_shape>
      alpha_times_log_beta_over_1p_beta(len_ab);
  for (size_t i = 0; i < len_ab; ++i)
    alpha_times_log_beta_over_1p_beta[i]
        = value_of(alpha_vec[i])
          * log(value_of(beta_vec[i]) / (1.0 + value_of(beta_vec[i])));

  VectorBuilder<!is_constant_struct<T_shape>::value, T_partials_return, T_shape>
      digamma_alpha(length(alpha));
  if (!is_constant_struct<T_shape>::value) {
    for (size_t i = 0; i < length(alpha); ++i)
      digamma_alpha[i] = digamma(value_of(alpha_vec[i]));
  }

  VectorBuilder<!is_constant_struct<T_shape>::value, T_partials_return,
                T_inv_scale>
      log_beta(length(beta));
  if (!is_constant_struct<T_shape>::value) {
    for (size_t i = 0; i < length(beta); ++i)
      log_beta[i] = log(value_of(beta_vec[i]));
  }

  VectorBuilder<!is_constant_struct<T_inv_scale>::value, T_partials_return,
                T_shape, T_inv_scale>
      lambda_m_alpha_over_1p_beta(len_ab);
  if (!is_constant_struct<T_inv_scale>::value) {
    for (size_t i = 0; i < len_ab; ++i)
      lambda_m_alpha_over_1p_beta[i]
          = lambda[i]
            - (value_of(alpha_vec[i]) / (1.0 + value_of(beta_vec[i])));
  }

  for (size_t i = 0; i < size; i++) {
    if (alpha_vec[i] > 1e10) {  // reduces numerically to Poisson
      if (include_summand<propto>::value)
        logp -= lgamma(n_vec[i] + 1.0);
      if (include_summand<propto, T_shape, T_inv_scale>::value)
        logp += multiply_log(n_vec[i], lambda[i]) - lambda[i];

      if (!is_constant_struct<T_shape>::value)
        ops_partials.edge1_.partials_[i]
            += n_vec[i] / value_of(alpha_vec[i]) - 1.0 / value_of(beta_vec[i]);
      if (!is_constant_struct<T_inv_scale>::value)
        ops_partials.edge2_.partials_[i]
            += (lambda[i] - n_vec[i]) / value_of(beta_vec[i]);
    } else {  // standard density definition
      if (include_summand<propto, T_shape>::value)
        if (n_vec[i] != 0)
          logp += binomial_coefficient_log(
              n_vec[i] + value_of(alpha_vec[i]) - 1.0, n_vec[i]);
      if (include_summand<propto, T_shape, T_inv_scale>::value)
        logp += alpha_times_log_beta_over_1p_beta[i] - n_vec[i] * log1p_beta[i];

      if (!is_constant_struct<T_shape>::value)
        ops_partials.edge1_.partials_[i]
            += digamma(value_of(alpha_vec[i]) + n_vec[i]) - digamma_alpha[i]
               + log_beta_m_log1p_beta[i];
      if (!is_constant_struct<T_inv_scale>::value)
        ops_partials.edge2_.partials_[i]
            += lambda_m_alpha_over_1p_beta[i]
               - n_vec[i] / (value_of(beta_vec[i]) + 1.0);
    }
  }
  return ops_partials.build(logp);
}

template <typename T_n, typename T_shape, typename T_inv_scale>
inline typename return_type<T_shape, T_inv_scale>::type neg_binomial_lpmf(
    const T_n& n, const T_shape& alpha, const T_inv_scale& beta) {
  return neg_binomial_lpmf<false>(n, alpha, beta);
}

}  // namespace math
}  // namespace stan
#endif
