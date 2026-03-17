#ifndef STAN_MATH_PRIM_SCAL_PROB_PARETO_CDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_PARETO_CDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_greater_or_equal.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/multiply_log.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <boost/random/exponential_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>
#include <limits>

namespace stan {
namespace math {

template <typename T_y, typename T_scale, typename T_shape>
typename return_type<T_y, T_scale, T_shape>::type pareto_cdf(
    const T_y& y, const T_scale& y_min, const T_shape& alpha) {
  typedef typename stan::partials_return_type<T_y, T_scale, T_shape>::type
      T_partials_return;

  if (size_zero(y, y_min, alpha))
    return 1.0;

  static const char* function = "pareto_cdf";

  using std::exp;
  using std::log;

  T_partials_return P(1.0);

  check_not_nan(function, "Random variable", y);
  check_nonnegative(function, "Random variable", y);
  check_positive_finite(function, "Scale parameter", y_min);
  check_positive_finite(function, "Shape parameter", alpha);
  check_consistent_sizes(function, "Random variable", y, "Scale parameter",
                         y_min, "Shape parameter", alpha);

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_scale> y_min_vec(y_min);
  scalar_seq_view<T_shape> alpha_vec(alpha);
  size_t N = max_size(y, y_min, alpha);

  operands_and_partials<T_y, T_scale, T_shape> ops_partials(y, y_min, alpha);

  // Explicit return for extreme values
  // The gradients are technically ill-defined, but treated as zero
  for (size_t i = 0; i < stan::length(y); i++) {
    if (value_of(y_vec[i]) < value_of(y_min_vec[i]))
      return ops_partials.build(0.0);
  }

  for (size_t n = 0; n < N; n++) {
    // Explicit results for extreme values
    // The gradients are technically ill-defined, but treated as zero
    if (value_of(y_vec[n]) == std::numeric_limits<double>::infinity()) {
      continue;
    }

    const T_partials_return log_dbl
        = log(value_of(y_min_vec[n]) / value_of(y_vec[n]));
    const T_partials_return y_min_inv_dbl = 1.0 / value_of(y_min_vec[n]);
    const T_partials_return alpha_dbl = value_of(alpha_vec[n]);

    const T_partials_return Pn = 1.0 - exp(alpha_dbl * log_dbl);

    P *= Pn;

    if (!is_constant_struct<T_y>::value)
      ops_partials.edge1_.partials_[n]
          += alpha_dbl * y_min_inv_dbl * exp((alpha_dbl + 1) * log_dbl) / Pn;
    if (!is_constant_struct<T_scale>::value)
      ops_partials.edge2_.partials_[n]
          += -alpha_dbl * y_min_inv_dbl * exp(alpha_dbl * log_dbl) / Pn;
    if (!is_constant_struct<T_shape>::value)
      ops_partials.edge3_.partials_[n]
          += -exp(alpha_dbl * log_dbl) * log_dbl / Pn;
  }

  if (!is_constant_struct<T_y>::value) {
    for (size_t n = 0; n < stan::length(y); ++n)
      ops_partials.edge1_.partials_[n] *= P;
  }
  if (!is_constant_struct<T_scale>::value) {
    for (size_t n = 0; n < stan::length(y_min); ++n)
      ops_partials.edge2_.partials_[n] *= P;
  }
  if (!is_constant_struct<T_shape>::value) {
    for (size_t n = 0; n < stan::length(alpha); ++n)
      ops_partials.edge3_.partials_[n] *= P;
  }
  return ops_partials.build(P);
}

}  // namespace math
}  // namespace stan
#endif
