#ifndef STAN_MATH_PRIM_SCAL_PROB_EXP_MOD_NORMAL_LCDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_EXP_MOD_NORMAL_LCDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/is_inf.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <boost/random/normal_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T_y, typename T_loc, typename T_scale, typename T_inv_scale>
typename return_type<T_y, T_loc, T_scale, T_inv_scale>::type
exp_mod_normal_lcdf(const T_y& y, const T_loc& mu, const T_scale& sigma,
                    const T_inv_scale& lambda) {
  static const char* function = "exp_mod_normal_lcdf";
  typedef
      typename stan::partials_return_type<T_y, T_loc, T_scale,
                                          T_inv_scale>::type T_partials_return;

  T_partials_return cdf_log(0.0);
  if (size_zero(y, mu, sigma, lambda))
    return cdf_log;

  check_not_nan(function, "Random variable", y);
  check_finite(function, "Location parameter", mu);
  check_not_nan(function, "Scale parameter", sigma);
  check_positive_finite(function, "Scale parameter", sigma);
  check_positive_finite(function, "Inv_scale parameter", lambda);
  check_not_nan(function, "Inv_scale parameter", lambda);
  check_consistent_sizes(function, "Random variable", y, "Location parameter",
                         mu, "Scale parameter", sigma, "Inv_scale paramter",
                         lambda);

  operands_and_partials<T_y, T_loc, T_scale, T_inv_scale> ops_partials(
      y, mu, sigma, lambda);

  using std::exp;
  using std::log;
  using std::log;

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_loc> mu_vec(mu);
  scalar_seq_view<T_scale> sigma_vec(sigma);
  scalar_seq_view<T_inv_scale> lambda_vec(lambda);
  size_t N = max_size(y, mu, sigma, lambda);
  const double sqrt_pi = std::sqrt(pi());
  for (size_t n = 0; n < N; n++) {
    if (is_inf(y_vec[n])) {
      if (y_vec[n] < 0.0)
        return ops_partials.build(negative_infinity());
      else
        return ops_partials.build(0.0);
    }

    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return mu_dbl = value_of(mu_vec[n]);
    const T_partials_return sigma_dbl = value_of(sigma_vec[n]);
    const T_partials_return lambda_dbl = value_of(lambda_vec[n]);
    const T_partials_return u = lambda_dbl * (y_dbl - mu_dbl);
    const T_partials_return v = lambda_dbl * sigma_dbl;
    const T_partials_return v_sq = v * v;
    const T_partials_return scaled_diff
        = (y_dbl - mu_dbl) / (SQRT_2 * sigma_dbl);
    const T_partials_return scaled_diff_sq = scaled_diff * scaled_diff;
    const T_partials_return erf_calc1 = 0.5 * (1 + erf(u / (v * SQRT_2)));
    const T_partials_return erf_calc2
        = 0.5 * (1 + erf(u / (v * SQRT_2) - v / SQRT_2));
    const T_partials_return deriv_1
        = lambda_dbl * exp(0.5 * v_sq - u) * erf_calc2;
    const T_partials_return deriv_2
        = SQRT_2 / sqrt_pi * 0.5
          * exp(0.5 * v_sq
                - (-scaled_diff + (v / SQRT_2)) * (-scaled_diff + (v / SQRT_2))
                - u)
          / sigma_dbl;
    const T_partials_return deriv_3
        = SQRT_2 / sqrt_pi * 0.5 * exp(-scaled_diff_sq) / sigma_dbl;

    const T_partials_return denom = erf_calc1 - erf_calc2 * exp(0.5 * v_sq - u);
    const T_partials_return cdf_
        = erf_calc1 - exp(-u + v_sq * 0.5) * (erf_calc2);

    cdf_log += log(cdf_);

    if (!is_constant_struct<T_y>::value)
      ops_partials.edge1_.partials_[n] += (deriv_1 - deriv_2 + deriv_3) / denom;
    if (!is_constant_struct<T_loc>::value)
      ops_partials.edge2_.partials_[n]
          += (-deriv_1 + deriv_2 - deriv_3) / denom;
    if (!is_constant_struct<T_scale>::value)
      ops_partials.edge3_.partials_[n]
          += (-deriv_1 * v - deriv_3 * scaled_diff * SQRT_2
              - deriv_2 * sigma_dbl * SQRT_2
                    * (-SQRT_2 * 0.5
                           * (-lambda_dbl + scaled_diff * SQRT_2 / sigma_dbl)
                       - SQRT_2 * lambda_dbl))
             / denom;
    if (!is_constant_struct<T_inv_scale>::value)
      ops_partials.edge4_.partials_[n]
          += exp(0.5 * v_sq - u)
             * (SQRT_2 / sqrt_pi * 0.5 * sigma_dbl
                    * exp(-(v / SQRT_2 - scaled_diff)
                          * (v / SQRT_2 - scaled_diff))
                - (v * sigma_dbl + mu_dbl - y_dbl) * erf_calc2)
             / denom;
  }
  return ops_partials.build(cdf_log);
}

}  // namespace math
}  // namespace stan
#endif
