#ifndef STAN_MATH_PRIM_SCAL_PROB_FRECHET_LPDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_FRECHET_LPDF_HPP

#include <boost/random/weibull_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/log1m.hpp>
#include <stan/math/prim/scal/fun/multiply_log.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <cmath>

namespace stan {
namespace math {

// Frechet(y|alpha, sigma)     [y > 0;  alpha > 0;  sigma > 0]
// FIXME: document
template <bool propto, typename T_y, typename T_shape, typename T_scale>
typename return_type<T_y, T_shape, T_scale>::type frechet_lpdf(
    const T_y& y, const T_shape& alpha, const T_scale& sigma) {
  static const char* function = "frechet_lpdf";
  typedef typename stan::partials_return_type<T_y, T_shape, T_scale>::type
      T_partials_return;

  using std::log;

  if (size_zero(y, alpha, sigma))
    return 0.0;

  T_partials_return logp(0.0);
  check_positive(function, "Random variable", y);
  check_positive_finite(function, "Shape parameter", alpha);
  check_positive_finite(function, "Scale parameter", sigma);
  check_consistent_sizes(function, "Random variable", y, "Shape parameter",
                         alpha, "Scale parameter", sigma);

  if (!include_summand<propto, T_y, T_shape, T_scale>::value)
    return 0.0;

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_shape> alpha_vec(alpha);
  scalar_seq_view<T_scale> sigma_vec(sigma);
  size_t N = max_size(y, alpha, sigma);

  VectorBuilder<include_summand<propto, T_shape>::value, T_partials_return,
                T_shape>
      log_alpha(length(alpha));
  for (size_t i = 0; i < length(alpha); i++)
    if (include_summand<propto, T_shape>::value)
      log_alpha[i] = log(value_of(alpha_vec[i]));

  VectorBuilder<include_summand<propto, T_y, T_shape>::value, T_partials_return,
                T_y>
      log_y(length(y));
  for (size_t i = 0; i < length(y); i++)
    if (include_summand<propto, T_y, T_shape>::value)
      log_y[i] = log(value_of(y_vec[i]));

  VectorBuilder<include_summand<propto, T_shape, T_scale>::value,
                T_partials_return, T_scale>
      log_sigma(length(sigma));
  for (size_t i = 0; i < length(sigma); i++)
    if (include_summand<propto, T_shape, T_scale>::value)
      log_sigma[i] = log(value_of(sigma_vec[i]));

  VectorBuilder<include_summand<propto, T_y, T_shape, T_scale>::value,
                T_partials_return, T_y>
      inv_y(length(y));
  for (size_t i = 0; i < length(y); i++)
    if (include_summand<propto, T_y, T_shape, T_scale>::value)
      inv_y[i] = 1.0 / value_of(y_vec[i]);

  VectorBuilder<include_summand<propto, T_y, T_shape, T_scale>::value,
                T_partials_return, T_y, T_shape, T_scale>
      sigma_div_y_pow_alpha(N);
  for (size_t i = 0; i < N; i++)
    if (include_summand<propto, T_y, T_shape, T_scale>::value) {
      const T_partials_return alpha_dbl = value_of(alpha_vec[i]);
      sigma_div_y_pow_alpha[i]
          = pow(inv_y[i] * value_of(sigma_vec[i]), alpha_dbl);
    }

  operands_and_partials<T_y, T_shape, T_scale> ops_partials(y, alpha, sigma);
  for (size_t n = 0; n < N; n++) {
    const T_partials_return alpha_dbl = value_of(alpha_vec[n]);
    if (include_summand<propto, T_shape>::value)
      logp += log_alpha[n];
    if (include_summand<propto, T_y, T_shape>::value)
      logp -= (alpha_dbl + 1.0) * log_y[n];
    if (include_summand<propto, T_shape, T_scale>::value)
      logp += alpha_dbl * log_sigma[n];
    if (include_summand<propto, T_y, T_shape, T_scale>::value)
      logp -= sigma_div_y_pow_alpha[n];

    if (!is_constant_struct<T_y>::value) {
      const T_partials_return inv_y_dbl = value_of(inv_y[n]);
      ops_partials.edge1_.partials_[n]
          += -(alpha_dbl + 1.0) * inv_y_dbl
             + alpha_dbl * sigma_div_y_pow_alpha[n] * inv_y_dbl;
    }
    if (!is_constant_struct<T_shape>::value)
      ops_partials.edge2_.partials_[n]
          += 1.0 / alpha_dbl
             + (1.0 - sigma_div_y_pow_alpha[n]) * (log_sigma[n] - log_y[n]);
    if (!is_constant_struct<T_scale>::value)
      ops_partials.edge3_.partials_[n] += alpha_dbl / value_of(sigma_vec[n])
                                          * (1 - sigma_div_y_pow_alpha[n]);
  }
  return ops_partials.build(logp);
}

template <typename T_y, typename T_shape, typename T_scale>
inline typename return_type<T_y, T_shape, T_scale>::type frechet_lpdf(
    const T_y& y, const T_shape& alpha, const T_scale& sigma) {
  return frechet_lpdf<false>(y, alpha, sigma);
}

}  // namespace math
}  // namespace stan
#endif
