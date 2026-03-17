#ifndef STAN_MATH_PRIM_SCAL_PROB_EXP_MOD_NORMAL_LPDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_EXP_MOD_NORMAL_LPDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/erfc.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <boost/random/normal_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>

namespace stan {
namespace math {

template <bool propto, typename T_y, typename T_loc, typename T_scale,
          typename T_inv_scale>
typename return_type<T_y, T_loc, T_scale, T_inv_scale>::type
exp_mod_normal_lpdf(const T_y& y, const T_loc& mu, const T_scale& sigma,
                    const T_inv_scale& lambda) {
  static const char* function = "exp_mod_normal_lpdf";
  typedef
      typename stan::partials_return_type<T_y, T_loc, T_scale,
                                          T_inv_scale>::type T_partials_return;

  using std::log;

  if (size_zero(y, mu, sigma, lambda))
    return 0.0;

  T_partials_return logp(0.0);

  check_not_nan(function, "Random variable", y);
  check_finite(function, "Location parameter", mu);
  check_positive_finite(function, "Inv_scale parameter", lambda);
  check_positive_finite(function, "Scale parameter", sigma);
  check_consistent_sizes(function, "Random variable", y, "Location parameter",
                         mu, "Scale parameter", sigma, "Inv_scale paramter",
                         lambda);

  if (!include_summand<propto, T_y, T_loc, T_scale, T_inv_scale>::value)
    return 0.0;

  using std::exp;
  using std::log;
  using std::sqrt;

  operands_and_partials<T_y, T_loc, T_scale, T_inv_scale> ops_partials(
      y, mu, sigma, lambda);

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_loc> mu_vec(mu);
  scalar_seq_view<T_scale> sigma_vec(sigma);
  scalar_seq_view<T_inv_scale> lambda_vec(lambda);
  size_t N = max_size(y, mu, sigma, lambda);

  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return mu_dbl = value_of(mu_vec[n]);
    const T_partials_return sigma_dbl = value_of(sigma_vec[n]);
    const T_partials_return lambda_dbl = value_of(lambda_vec[n]);

    const T_partials_return pi_dbl = boost::math::constants::pi<double>();

    if (include_summand<propto>::value)
      logp -= log(2.0);
    if (include_summand<propto, T_inv_scale>::value)
      logp += log(lambda_dbl);
    if (include_summand<propto, T_y, T_loc, T_scale, T_inv_scale>::value)
      logp += lambda_dbl
                  * (mu_dbl + 0.5 * lambda_dbl * sigma_dbl * sigma_dbl - y_dbl)
              + log(erfc((mu_dbl + lambda_dbl * sigma_dbl * sigma_dbl - y_dbl)
                         / (sqrt(2.0) * sigma_dbl)));

    const T_partials_return deriv_logerfc
        = -2.0 / sqrt(pi_dbl)
          * exp(-(mu_dbl + lambda_dbl * sigma_dbl * sigma_dbl - y_dbl)
                / (std::sqrt(2.0) * sigma_dbl)
                * (mu_dbl + lambda_dbl * sigma_dbl * sigma_dbl - y_dbl)
                / (sigma_dbl * std::sqrt(2.0)))
          / erfc((mu_dbl + lambda_dbl * sigma_dbl * sigma_dbl - y_dbl)
                 / (sigma_dbl * std::sqrt(2.0)));

    if (!is_constant_struct<T_y>::value)
      ops_partials.edge1_.partials_[n]
          += -lambda_dbl + deriv_logerfc * -1.0 / (sigma_dbl * std::sqrt(2.0));
    if (!is_constant_struct<T_loc>::value)
      ops_partials.edge2_.partials_[n]
          += lambda_dbl + deriv_logerfc / (sigma_dbl * std::sqrt(2.0));
    if (!is_constant_struct<T_scale>::value)
      ops_partials.edge3_.partials_[n]
          += sigma_dbl * lambda_dbl * lambda_dbl
             + deriv_logerfc
                   * (-mu_dbl / (sigma_dbl * sigma_dbl * std::sqrt(2.0))
                      + lambda_dbl / std::sqrt(2.0)
                      + y_dbl / (sigma_dbl * sigma_dbl * std::sqrt(2.0)));
    if (!is_constant_struct<T_inv_scale>::value)
      ops_partials.edge4_.partials_[n]
          += 1 / lambda_dbl + lambda_dbl * sigma_dbl * sigma_dbl + mu_dbl
             - y_dbl + deriv_logerfc * sigma_dbl / std::sqrt(2.0);
  }
  return ops_partials.build(logp);
}

template <typename T_y, typename T_loc, typename T_scale, typename T_inv_scale>
inline typename return_type<T_y, T_loc, T_scale, T_inv_scale>::type
exp_mod_normal_lpdf(const T_y& y, const T_loc& mu, const T_scale& sigma,
                    const T_inv_scale& lambda) {
  return exp_mod_normal_lpdf<false>(y, mu, sigma, lambda);
}

}  // namespace math
}  // namespace stan
#endif
