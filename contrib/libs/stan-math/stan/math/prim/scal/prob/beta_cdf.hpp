#ifndef STAN_MATH_PRIM_SCAL_PROB_BETA_CDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BETA_CDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/contains_nonconstant_struct.hpp>
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
#include <stan/math/prim/scal/fun/inc_beta.hpp>
#include <stan/math/prim/scal/fun/inc_beta_dda.hpp>
#include <stan/math/prim/scal/fun/inc_beta_ddb.hpp>
#include <stan/math/prim/scal/fun/inc_beta_ddz.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>
#include <stan/math/prim/scal/fun/lbeta.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <boost/math/special_functions/gamma.hpp>
#include <boost/random/gamma_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Calculates the beta cumulative distribution function for the given
 * variate and scale variables.
 *
 * @param y A scalar variate.
 * @param alpha Prior sample size.
 * @param beta Prior sample size.
 * @return The beta cdf evaluated at the specified arguments.
 * @tparam T_y Type of y.
 * @tparam T_scale_succ Type of alpha.
 * @tparam T_scale_fail Type of beta.
 */
template <typename T_y, typename T_scale_succ, typename T_scale_fail>
typename return_type<T_y, T_scale_succ, T_scale_fail>::type beta_cdf(
    const T_y& y, const T_scale_succ& alpha, const T_scale_fail& beta) {
  typedef
      typename stan::partials_return_type<T_y, T_scale_succ, T_scale_fail>::type
          T_partials_return;

  if (size_zero(y, alpha, beta))
    return 1.0;

  static const char* function = "beta_cdf";
  using boost::math::tools::promote_args;

  T_partials_return P(1.0);

  check_positive_finite(function, "First shape parameter", alpha);
  check_positive_finite(function, "Second shape parameter", beta);
  check_not_nan(function, "Random variable", y);
  check_consistent_sizes(function, "Random variable", y,
                         "First shape parameter", alpha,
                         "Second shape parameter", beta);
  check_nonnegative(function, "Random variable", y);
  check_less_or_equal(function, "Random variable", y, 1);

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_scale_succ> alpha_vec(alpha);
  scalar_seq_view<T_scale_fail> beta_vec(beta);
  size_t N = max_size(y, alpha, beta);

  operands_and_partials<T_y, T_scale_succ, T_scale_fail> ops_partials(y, alpha,
                                                                      beta);

  // Explicit return for extreme values
  // The gradients are technically ill-defined, but treated as zero
  for (size_t i = 0; i < stan::length(y); i++) {
    if (value_of(y_vec[i]) <= 0)
      return ops_partials.build(0.0);
  }

  VectorBuilder<contains_nonconstant_struct<T_scale_succ, T_scale_fail>::value,
                T_partials_return, T_scale_succ, T_scale_fail>
      digamma_alpha_vec(max_size(alpha, beta));

  VectorBuilder<contains_nonconstant_struct<T_scale_succ, T_scale_fail>::value,
                T_partials_return, T_scale_succ, T_scale_fail>
      digamma_beta_vec(max_size(alpha, beta));

  VectorBuilder<contains_nonconstant_struct<T_scale_succ, T_scale_fail>::value,
                T_partials_return, T_scale_succ, T_scale_fail>
      digamma_sum_vec(max_size(alpha, beta));

  if (contains_nonconstant_struct<T_scale_succ, T_scale_fail>::value) {
    for (size_t n = 0; n < N; n++) {
      const T_partials_return alpha_dbl = value_of(alpha_vec[n]);
      const T_partials_return beta_dbl = value_of(beta_vec[n]);

      digamma_alpha_vec[n] = digamma(alpha_dbl);
      digamma_beta_vec[n] = digamma(beta_dbl);
      digamma_sum_vec[n] = digamma(alpha_dbl + beta_dbl);
    }
  }

  for (size_t n = 0; n < N; n++) {
    // Explicit results for extreme values
    // The gradients are technically ill-defined, but treated as zero
    if (value_of(y_vec[n]) >= 1.0)
      continue;

    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return alpha_dbl = value_of(alpha_vec[n]);
    const T_partials_return beta_dbl = value_of(beta_vec[n]);

    const T_partials_return Pn = inc_beta(alpha_dbl, beta_dbl, y_dbl);

    P *= Pn;

    if (!is_constant_struct<T_y>::value)
      ops_partials.edge1_.partials_[n]
          += inc_beta_ddz(alpha_dbl, beta_dbl, y_dbl) / Pn;

    if (!is_constant_struct<T_scale_succ>::value)
      ops_partials.edge2_.partials_[n]
          += inc_beta_dda(alpha_dbl, beta_dbl, y_dbl, digamma_alpha_vec[n],
                          digamma_sum_vec[n])
             / Pn;
    if (!is_constant_struct<T_scale_fail>::value)
      ops_partials.edge3_.partials_[n]
          += inc_beta_ddb(alpha_dbl, beta_dbl, y_dbl, digamma_beta_vec[n],
                          digamma_sum_vec[n])
             / Pn;
  }

  if (!is_constant_struct<T_y>::value) {
    for (size_t n = 0; n < stan::length(y); ++n)
      ops_partials.edge1_.partials_[n] *= P;
  }
  if (!is_constant_struct<T_scale_succ>::value) {
    for (size_t n = 0; n < stan::length(alpha); ++n)
      ops_partials.edge2_.partials_[n] *= P;
  }
  if (!is_constant_struct<T_scale_fail>::value) {
    for (size_t n = 0; n < stan::length(beta); ++n)
      ops_partials.edge3_.partials_[n] *= P;
  }

  return ops_partials.build(P);
}

}  // namespace math
}  // namespace stan
#endif
