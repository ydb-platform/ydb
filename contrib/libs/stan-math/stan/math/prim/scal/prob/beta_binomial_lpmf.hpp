#ifndef STAN_MATH_PRIM_SCAL_PROB_BETA_BINOMIAL_LPMF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BETA_BINOMIAL_LPMF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/lbeta.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>
#include <stan/math/prim/scal/fun/binomial_coefficient_log.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/contains_nonconstant_struct.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/prob/beta_rng.hpp>
#include <stan/math/prim/scal/fun/F32.hpp>
#include <stan/math/prim/scal/fun/grad_F32.hpp>

namespace stan {
namespace math {

/**
 * Returns the log PMF of the Beta-Binomial distribution with given population
 * size, prior success, and prior failure parameters. Given containers of
 * matching sizes, returns the log sum of probabilities.
 *
 * @tparam T_n type of success parameter
 * @tparam T_N type of population size parameter
 * @tparam T_size1 type of prior success parameter
 * @tparam T_size2 type of prior failure parameter
 * @param n success parameter
 * @param N population size parameter
 * @param alpha prior success parameter
 * @param beta prior failure parameter
 * @return log probability or log sum of probabilities
 * @throw std::domain_error if N, alpha, or beta fails to be positive
 * @throw std::invalid_argument if container sizes mismatch
 */
template <bool propto, typename T_n, typename T_N, typename T_size1,
          typename T_size2>
typename return_type<T_size1, T_size2>::type beta_binomial_lpmf(
    const T_n& n, const T_N& N, const T_size1& alpha, const T_size2& beta) {
  static const char* function = "beta_binomial_lpmf";
  typedef typename stan::partials_return_type<T_size1, T_size2>::type
      T_partials_return;

  if (size_zero(n, N, alpha, beta))
    return 0.0;

  T_partials_return logp(0.0);
  check_nonnegative(function, "Population size parameter", N);
  check_positive_finite(function, "First prior sample size parameter", alpha);
  check_positive_finite(function, "Second prior sample size parameter", beta);
  check_consistent_sizes(function, "Successes variable", n,
                         "Population size parameter", N,
                         "First prior sample size parameter", alpha,
                         "Second prior sample size parameter", beta);

  if (!include_summand<propto, T_size1, T_size2>::value)
    return 0.0;

  operands_and_partials<T_size1, T_size2> ops_partials(alpha, beta);

  scalar_seq_view<T_n> n_vec(n);
  scalar_seq_view<T_N> N_vec(N);
  scalar_seq_view<T_size1> alpha_vec(alpha);
  scalar_seq_view<T_size2> beta_vec(beta);
  size_t size = max_size(n, N, alpha, beta);

  for (size_t i = 0; i < size; i++) {
    if (n_vec[i] < 0 || n_vec[i] > N_vec[i])
      return ops_partials.build(LOG_ZERO);
  }

  VectorBuilder<include_summand<propto>::value, T_partials_return, T_n, T_N>
      normalizing_constant(max_size(N, n));
  for (size_t i = 0; i < max_size(N, n); i++)
    if (include_summand<propto>::value)
      normalizing_constant[i] = binomial_coefficient_log(N_vec[i], n_vec[i]);

  VectorBuilder<include_summand<propto, T_size1, T_size2>::value,
                T_partials_return, T_n, T_N, T_size1, T_size2>
      lbeta_numerator(size);
  for (size_t i = 0; i < size; i++)
    if (include_summand<propto, T_size1, T_size2>::value)
      lbeta_numerator[i] = lbeta(n_vec[i] + value_of(alpha_vec[i]),
                                 N_vec[i] - n_vec[i] + value_of(beta_vec[i]));

  VectorBuilder<include_summand<propto, T_size1, T_size2>::value,
                T_partials_return, T_size1, T_size2>
      lbeta_denominator(max_size(alpha, beta));
  for (size_t i = 0; i < max_size(alpha, beta); i++)
    if (include_summand<propto, T_size1, T_size2>::value)
      lbeta_denominator[i]
          = lbeta(value_of(alpha_vec[i]), value_of(beta_vec[i]));

  VectorBuilder<!is_constant_struct<T_size1>::value, T_partials_return, T_n,
                T_size1>
      digamma_n_plus_alpha(max_size(n, alpha));
  for (size_t i = 0; i < max_size(n, alpha); i++)
    if (!is_constant_struct<T_size1>::value)
      digamma_n_plus_alpha[i] = digamma(n_vec[i] + value_of(alpha_vec[i]));

  VectorBuilder<contains_nonconstant_struct<T_size1, T_size2>::value,
                T_partials_return, T_N, T_size1, T_size2>
      digamma_N_plus_alpha_plus_beta(max_size(N, alpha, beta));
  for (size_t i = 0; i < max_size(N, alpha, beta); i++)
    if (contains_nonconstant_struct<T_size1, T_size2>::value)
      digamma_N_plus_alpha_plus_beta[i]
          = digamma(N_vec[i] + value_of(alpha_vec[i]) + value_of(beta_vec[i]));

  VectorBuilder<contains_nonconstant_struct<T_size1, T_size2>::value,
                T_partials_return, T_size1, T_size2>
      digamma_alpha_plus_beta(max_size(alpha, beta));
  for (size_t i = 0; i < max_size(alpha, beta); i++)
    if (contains_nonconstant_struct<T_size1, T_size2>::value)
      digamma_alpha_plus_beta[i]
          = digamma(value_of(alpha_vec[i]) + value_of(beta_vec[i]));

  VectorBuilder<!is_constant_struct<T_size1>::value, T_partials_return, T_size1>
      digamma_alpha(length(alpha));
  for (size_t i = 0; i < length(alpha); i++)
    if (!is_constant_struct<T_size1>::value)
      digamma_alpha[i] = digamma(value_of(alpha_vec[i]));

  VectorBuilder<!is_constant_struct<T_size2>::value, T_partials_return, T_size2>
      digamma_beta(length(beta));
  for (size_t i = 0; i < length(beta); i++)
    if (!is_constant_struct<T_size2>::value)
      digamma_beta[i] = digamma(value_of(beta_vec[i]));

  for (size_t i = 0; i < size; i++) {
    if (include_summand<propto>::value)
      logp += normalizing_constant[i];
    if (include_summand<propto, T_size1, T_size2>::value)
      logp += lbeta_numerator[i] - lbeta_denominator[i];

    if (!is_constant_struct<T_size1>::value)
      ops_partials.edge1_.partials_[i]
          += digamma_n_plus_alpha[i] - digamma_N_plus_alpha_plus_beta[i]
             + digamma_alpha_plus_beta[i] - digamma_alpha[i];
    if (!is_constant_struct<T_size2>::value)
      ops_partials.edge2_.partials_[i]
          += digamma(value_of(N_vec[i] - n_vec[i] + beta_vec[i]))
             - digamma_N_plus_alpha_plus_beta[i] + digamma_alpha_plus_beta[i]
             - digamma_beta[i];
  }
  return ops_partials.build(logp);
}

template <typename T_n, typename T_N, typename T_size1, typename T_size2>
typename return_type<T_size1, T_size2>::type beta_binomial_lpmf(
    const T_n& n, const T_N& N, const T_size1& alpha, const T_size2& beta) {
  return beta_binomial_lpmf<false>(n, N, alpha, beta);
}

}  // namespace math
}  // namespace stan
#endif
