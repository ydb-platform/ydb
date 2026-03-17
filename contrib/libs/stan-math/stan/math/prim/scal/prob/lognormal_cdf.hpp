#ifndef STAN_MATH_PRIM_SCAL_PROB_LOGNORMAL_CDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_LOGNORMAL_CDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <boost/random/lognormal_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T_y, typename T_loc, typename T_scale>
typename return_type<T_y, T_loc, T_scale>::type lognormal_cdf(
    const T_y& y, const T_loc& mu, const T_scale& sigma) {
  static const char* function = "lognormal_cdf";

  typedef typename stan::partials_return_type<T_y, T_loc, T_scale>::type
      T_partials_return;

  T_partials_return cdf = 1.0;

  using boost::math::tools::promote_args;
  using std::exp;
  using std::log;

  if (size_zero(y, mu, sigma))
    return cdf;

  check_not_nan(function, "Random variable", y);
  check_nonnegative(function, "Random variable", y);
  check_finite(function, "Location parameter", mu);
  check_positive_finite(function, "Scale parameter", sigma);

  operands_and_partials<T_y, T_loc, T_scale> ops_partials(y, mu, sigma);

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_loc> mu_vec(mu);
  scalar_seq_view<T_scale> sigma_vec(sigma);
  size_t N = max_size(y, mu, sigma);

  const double sqrt_pi = std::sqrt(pi());

  for (size_t i = 0; i < stan::length(y); i++) {
    if (value_of(y_vec[i]) == 0.0)
      return ops_partials.build(0.0);
  }

  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return mu_dbl = value_of(mu_vec[n]);
    const T_partials_return sigma_dbl = value_of(sigma_vec[n]);
    const T_partials_return scaled_diff
        = (log(y_dbl) - mu_dbl) / (sigma_dbl * SQRT_2);
    const T_partials_return rep_deriv
        = SQRT_2 * 0.5 / sqrt_pi * exp(-scaled_diff * scaled_diff) / sigma_dbl;

    const T_partials_return cdf_ = 0.5 * erfc(-scaled_diff);
    cdf *= cdf_;

    if (!is_constant_struct<T_y>::value)
      ops_partials.edge1_.partials_[n] += rep_deriv / cdf_ / y_dbl;
    if (!is_constant_struct<T_loc>::value)
      ops_partials.edge2_.partials_[n] -= rep_deriv / cdf_;
    if (!is_constant_struct<T_scale>::value)
      ops_partials.edge3_.partials_[n]
          -= rep_deriv * scaled_diff * SQRT_2 / cdf_;
  }

  if (!is_constant_struct<T_y>::value) {
    for (size_t n = 0; n < stan::length(y); ++n)
      ops_partials.edge1_.partials_[n] *= cdf;
  }
  if (!is_constant_struct<T_loc>::value) {
    for (size_t n = 0; n < stan::length(mu); ++n)
      ops_partials.edge2_.partials_[n] *= cdf;
  }
  if (!is_constant_struct<T_scale>::value) {
    for (size_t n = 0; n < stan::length(sigma); ++n)
      ops_partials.edge3_.partials_[n] *= cdf;
  }
  return ops_partials.build(cdf);
}

}  // namespace math
}  // namespace stan
#endif
