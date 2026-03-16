#ifndef STAN_MATH_PRIM_SCAL_PROB_PARETO_TYPE_2_LCCDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_PARETO_TYPE_2_LCCDF_HPP

#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_greater_or_equal.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/fun/log1m.hpp>
#include <stan/math/prim/scal/meta/contains_nonconstant_struct.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T_y, typename T_loc, typename T_scale, typename T_shape>
typename return_type<T_y, T_loc, T_scale, T_shape>::type pareto_type_2_lccdf(
    const T_y& y, const T_loc& mu, const T_scale& lambda,
    const T_shape& alpha) {
  typedef
      typename stan::partials_return_type<T_y, T_loc, T_scale, T_shape>::type
          T_partials_return;

  if (size_zero(y, mu, lambda, alpha))
    return 0.0;

  static const char* function = "pareto_type_2_lccdf";

  using std::log;

  T_partials_return P(0.0);

  check_greater_or_equal(function, "Random variable", y, mu);
  check_not_nan(function, "Random variable", y);
  check_nonnegative(function, "Random variable", y);
  check_positive_finite(function, "Scale parameter", lambda);
  check_positive_finite(function, "Shape parameter", alpha);
  check_consistent_sizes(function, "Random variable", y, "Scale parameter",
                         lambda, "Shape parameter", alpha);

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_loc> mu_vec(mu);
  scalar_seq_view<T_scale> lambda_vec(lambda);
  scalar_seq_view<T_shape> alpha_vec(alpha);
  size_t N = max_size(y, mu, lambda, alpha);

  operands_and_partials<T_y, T_loc, T_scale, T_shape> ops_partials(
      y, mu, lambda, alpha);

  VectorBuilder<true, T_partials_return, T_y, T_loc, T_scale, T_shape> ccdf_log(
      N);

  VectorBuilder<
      contains_nonconstant_struct<T_y, T_loc, T_scale, T_shape>::value,
      T_partials_return, T_y, T_loc, T_scale, T_shape>
      a_over_lambda_plus_y(N);

  VectorBuilder<!is_constant_struct<T_shape>::value, T_partials_return, T_y,
                T_loc, T_scale, T_shape>
      log_1p_y_over_lambda(N);

  for (size_t i = 0; i < N; i++) {
    const T_partials_return y_dbl = value_of(y_vec[i]);
    const T_partials_return mu_dbl = value_of(mu_vec[i]);
    const T_partials_return lambda_dbl = value_of(lambda_vec[i]);
    const T_partials_return alpha_dbl = value_of(alpha_vec[i]);
    const T_partials_return temp = 1.0 + (y_dbl - mu_dbl) / lambda_dbl;
    const T_partials_return log_temp = log(temp);

    ccdf_log[i] = -alpha_dbl * log_temp;

    if (contains_nonconstant_struct<T_y, T_loc, T_scale, T_shape>::value)
      a_over_lambda_plus_y[i] = alpha_dbl / (y_dbl - mu_dbl + lambda_dbl);

    if (!is_constant_struct<T_shape>::value)
      log_1p_y_over_lambda[i] = log_temp;
  }

  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return mu_dbl = value_of(mu_vec[n]);
    const T_partials_return lambda_dbl = value_of(lambda_vec[n]);

    P += ccdf_log[n];

    if (!is_constant_struct<T_y>::value)
      ops_partials.edge1_.partials_[n] -= a_over_lambda_plus_y[n];
    if (!is_constant_struct<T_loc>::value)
      ops_partials.edge2_.partials_[n] += a_over_lambda_plus_y[n];
    if (!is_constant_struct<T_scale>::value)
      ops_partials.edge3_.partials_[n]
          += a_over_lambda_plus_y[n] * (y_dbl - mu_dbl) / lambda_dbl;
    if (!is_constant_struct<T_shape>::value)
      ops_partials.edge4_.partials_[n] -= log_1p_y_over_lambda[n];
  }
  return ops_partials.build(P);
}

}  // namespace math
}  // namespace stan
#endif
