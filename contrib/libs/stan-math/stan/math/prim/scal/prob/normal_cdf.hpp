#ifndef STAN_MATH_PRIM_SCAL_PROB_NORMAL_CDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_NORMAL_CDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/meta/contains_nonconstant_struct.hpp>
#include <stan/math/prim/scal/meta/max_size.hpp>
#include <boost/random/normal_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Calculates the normal cumulative distribution function for the given
 * variate, location, and scale.
 *
 * \f$\Phi(x) = \frac{1}{\sqrt{2 \pi}} \int_{-\inf}^x e^{-t^2/2} dt\f$.
 *
 * @param y A scalar variate.
 * @param mu The location of the normal distribution.
 * @param sigma The scale of the normal distriubtion
 * @return The unit normal cdf evaluated at the specified arguments.
 * @tparam T_y Type of y.
 * @tparam T_loc Type of mean parameter.
 * @tparam T_scale Type of standard deviation paramater.
 */
template <typename T_y, typename T_loc, typename T_scale>
typename return_type<T_y, T_loc, T_scale>::type normal_cdf(
    const T_y& y, const T_loc& mu, const T_scale& sigma) {
  static const char* function = "normal_cdf";
  typedef typename stan::partials_return_type<T_y, T_loc, T_scale>::type
      T_partials_return;

  using std::exp;

  T_partials_return cdf(1.0);

  if (size_zero(y, mu, sigma))
    return cdf;

  check_not_nan(function, "Random variable", y);
  check_finite(function, "Location parameter", mu);
  check_not_nan(function, "Scale parameter", sigma);
  check_positive(function, "Scale parameter", sigma);
  check_consistent_sizes(function, "Random variable", y, "Location parameter",
                         mu, "Scale parameter", sigma);

  operands_and_partials<T_y, T_loc, T_scale> ops_partials(y, mu, sigma);

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_loc> mu_vec(mu);
  scalar_seq_view<T_scale> sigma_vec(sigma);
  size_t N = max_size(y, mu, sigma);
  const double SQRT_TWO_OVER_PI = std::sqrt(2.0 / pi());

  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return mu_dbl = value_of(mu_vec[n]);
    const T_partials_return sigma_dbl = value_of(sigma_vec[n]);
    const T_partials_return scaled_diff
        = (y_dbl - mu_dbl) / (sigma_dbl * SQRT_2);
    T_partials_return cdf_;
    if (scaled_diff < -37.5 * INV_SQRT_2)
      cdf_ = 0.0;
    else if (scaled_diff < -5.0 * INV_SQRT_2)
      cdf_ = 0.5 * erfc(-scaled_diff);
    else if (scaled_diff > 8.25 * INV_SQRT_2)
      cdf_ = 1;
    else
      cdf_ = 0.5 * (1.0 + erf(scaled_diff));

    cdf *= cdf_;

    if (contains_nonconstant_struct<T_y, T_loc, T_scale>::value) {
      const T_partials_return rep_deriv
          = (scaled_diff < -37.5 * INV_SQRT_2)
                ? 0.0
                : SQRT_TWO_OVER_PI * 0.5 * exp(-scaled_diff * scaled_diff)
                      / cdf_ / sigma_dbl;
      if (!is_constant_struct<T_y>::value)
        ops_partials.edge1_.partials_[n] += rep_deriv;
      if (!is_constant_struct<T_loc>::value)
        ops_partials.edge2_.partials_[n] -= rep_deriv;
      if (!is_constant_struct<T_scale>::value)
        ops_partials.edge3_.partials_[n] -= rep_deriv * scaled_diff * SQRT_2;
    }
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
