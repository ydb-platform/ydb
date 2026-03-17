#ifndef STAN_MATH_PRIM_SCAL_PROB_NORMAL_LCDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_NORMAL_LCDF_HPP

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
#include <stan/math/prim/scal/meta/max_size.hpp>
#include <stan/math/prim/scal/meta/contains_nonconstant_struct.hpp>
#include <boost/random/normal_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>
#include <limits>

namespace stan {
namespace math {

template <typename T_y, typename T_loc, typename T_scale>
typename return_type<T_y, T_loc, T_scale>::type normal_lcdf(
    const T_y& y, const T_loc& mu, const T_scale& sigma) {
  static const char* function = "normal_lcdf";
  typedef typename stan::partials_return_type<T_y, T_loc, T_scale>::type
      T_partials_return;

  using std::exp;
  using std::log;

  T_partials_return cdf_log(0.0);
  if (size_zero(y, mu, sigma))
    return cdf_log;

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

    T_partials_return one_p_erf;
    if (scaled_diff < -37.5 * INV_SQRT_2)
      one_p_erf = 0.0;
    else if (scaled_diff < -5.0 * INV_SQRT_2)
      one_p_erf = erfc(-scaled_diff);
    else if (scaled_diff > 8.25 * INV_SQRT_2)
      one_p_erf = 2.0;
    else
      one_p_erf = 1.0 + erf(scaled_diff);

    cdf_log += LOG_HALF + log(one_p_erf);

    if (contains_nonconstant_struct<T_y, T_loc, T_scale>::value) {
      const T_partials_return rep_deriv_div_sigma
          = scaled_diff < -37.5 * INV_SQRT_2
                ? std::numeric_limits<double>::infinity()
                : SQRT_TWO_OVER_PI * exp(-scaled_diff * scaled_diff) / sigma_dbl
                      / one_p_erf;
      if (!is_constant_struct<T_y>::value)
        ops_partials.edge1_.partials_[n] += rep_deriv_div_sigma;
      if (!is_constant_struct<T_loc>::value)
        ops_partials.edge2_.partials_[n] -= rep_deriv_div_sigma;
      if (!is_constant_struct<T_scale>::value)
        ops_partials.edge3_.partials_[n]
            -= rep_deriv_div_sigma * scaled_diff * SQRT_2;
    }
  }
  return ops_partials.build(cdf_log);
}

}  // namespace math
}  // namespace stan
#endif
