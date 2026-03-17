#ifndef STAN_MATH_PRIM_SCAL_PROB_DOUBLE_EXPONENTIAL_CDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_DOUBLE_EXPONENTIAL_CDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/fun/log1m.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/fun/sign.hpp>
#include <boost/random/uniform_01.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Returns the double exponential cumulative density function. Given
 * containers of matching sizes, returns the product of probabilities.
 *
 * @tparam T_y type of real parameter.
 * @tparam T_loc type of location parameter.
 * @tparam T_scale type of scale parameter.
 * @param y real parameter
 * @param mu location parameter
 * @param sigma scale parameter
 * @return probability or product of probabilities
 * @throw std::domain_error if y is nan, mu is infinite,
 *  or sigma is nonpositive
 */
template <typename T_y, typename T_loc, typename T_scale>
typename return_type<T_y, T_loc, T_scale>::type double_exponential_cdf(
    const T_y& y, const T_loc& mu, const T_scale& sigma) {
  static const char* function = "double_exponential_cdf";
  typedef typename stan::partials_return_type<T_y, T_loc, T_scale>::type
      T_partials_return;

  if (size_zero(y, mu, sigma))
    return 1.0;

  using boost::math::tools::promote_args;
  using std::exp;

  T_partials_return cdf(1.0);

  check_not_nan(function, "Random variable", y);
  check_finite(function, "Location parameter", mu);
  check_positive_finite(function, "Scale parameter", sigma);

  operands_and_partials<T_y, T_loc, T_scale> ops_partials(y, mu, sigma);

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_loc> mu_vec(mu);
  scalar_seq_view<T_scale> sigma_vec(sigma);
  size_t N = max_size(y, mu, sigma);

  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return mu_dbl = value_of(mu_vec[n]);
    const T_partials_return sigma_dbl = value_of(sigma_vec[n]);
    const T_partials_return scaled_diff = (y_dbl - mu_dbl) / (sigma_dbl);
    const T_partials_return exp_scaled_diff = exp(scaled_diff);

    if (y_dbl < mu_dbl)
      cdf *= exp_scaled_diff * 0.5;
    else
      cdf *= 1.0 - 0.5 / exp_scaled_diff;
  }

  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return mu_dbl = value_of(mu_vec[n]);
    const T_partials_return sigma_dbl = value_of(sigma_vec[n]);
    const T_partials_return scaled_diff = (y_dbl - mu_dbl) / sigma_dbl;
    const T_partials_return exp_scaled_diff = exp(scaled_diff);
    const T_partials_return inv_sigma = 1.0 / sigma_dbl;

    if (y_dbl < mu_dbl) {
      if (!is_constant_struct<T_y>::value)
        ops_partials.edge1_.partials_[n] += inv_sigma * cdf;
      if (!is_constant_struct<T_loc>::value)
        ops_partials.edge2_.partials_[n] -= inv_sigma * cdf;
      if (!is_constant_struct<T_scale>::value)
        ops_partials.edge3_.partials_[n] -= scaled_diff * inv_sigma * cdf;
    } else {
      const T_partials_return rep_deriv
          = cdf * inv_sigma / (2.0 * exp_scaled_diff - 1.0);
      if (!is_constant_struct<T_y>::value)
        ops_partials.edge1_.partials_[n] += rep_deriv;
      if (!is_constant_struct<T_loc>::value)
        ops_partials.edge2_.partials_[n] -= rep_deriv;
      if (!is_constant_struct<T_scale>::value)
        ops_partials.edge3_.partials_[n] -= rep_deriv * scaled_diff;
    }
  }
  return ops_partials.build(cdf);
}

}  // namespace math
}  // namespace stan
#endif
