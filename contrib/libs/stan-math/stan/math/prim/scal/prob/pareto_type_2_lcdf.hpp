#ifndef STAN_MATH_PRIM_SCAL_PROB_PARETO_TYPE_2_LCDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_PARETO_TYPE_2_LCDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
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
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T_y, typename T_loc, typename T_scale, typename T_shape>
typename return_type<T_y, T_loc, T_scale, T_shape>::type pareto_type_2_lcdf(
    const T_y& y, const T_loc& mu, const T_scale& lambda,
    const T_shape& alpha) {
  typedef
      typename stan::partials_return_type<T_y, T_loc, T_scale, T_shape>::type
          T_partials_return;

  if (size_zero(y, mu, lambda, alpha))
    return 0.0;

  static const char* function = "pareto_type_2_lcdf";

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

  VectorBuilder<true, T_partials_return, T_y, T_loc, T_scale, T_shape> cdf_log(
      N);

  VectorBuilder<true, T_partials_return, T_y, T_loc, T_scale, T_shape>
      inv_p1_pow_alpha_minus_one(N);

  VectorBuilder<!is_constant_struct<T_shape>::value, T_partials_return, T_y,
                T_loc, T_scale, T_shape>
      log_1p_y_over_lambda(N);

  for (size_t i = 0; i < N; i++) {
    const T_partials_return temp = 1.0
                                   + (value_of(y_vec[i]) - value_of(mu_vec[i]))
                                         / value_of(lambda_vec[i]);
    const T_partials_return p1_pow_alpha = pow(temp, value_of(alpha_vec[i]));
    cdf_log[i] = log1m(1.0 / p1_pow_alpha);

    inv_p1_pow_alpha_minus_one[i] = 1.0 / (p1_pow_alpha - 1.0);

    if (!is_constant_struct<T_shape>::value)
      log_1p_y_over_lambda[i] = log(temp);
  }

  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return mu_dbl = value_of(mu_vec[n]);
    const T_partials_return lambda_dbl = value_of(lambda_vec[n]);
    const T_partials_return alpha_dbl = value_of(alpha_vec[n]);

    const T_partials_return grad_1_2 = alpha_dbl * inv_p1_pow_alpha_minus_one[n]
                                       / (lambda_dbl - mu_dbl + y_dbl);

    P += cdf_log[n];

    if (!is_constant_struct<T_y>::value)
      ops_partials.edge1_.partials_[n] += grad_1_2;
    if (!is_constant_struct<T_loc>::value)
      ops_partials.edge2_.partials_[n] -= grad_1_2;
    if (!is_constant_struct<T_scale>::value)
      ops_partials.edge3_.partials_[n]
          += (mu_dbl - y_dbl) * grad_1_2 / lambda_dbl;
    if (!is_constant_struct<T_shape>::value)
      ops_partials.edge4_.partials_[n]
          += log_1p_y_over_lambda[n] * inv_p1_pow_alpha_minus_one[n];
  }
  return ops_partials.build(P);
}

}  // namespace math
}  // namespace stan
#endif
