#ifndef STAN_MATH_PRIM_SCAL_PROB_STUDENT_T_LPDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_STUDENT_T_LPDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/fun/lbeta.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <stan/math/prim/scal/fun/grad_reg_inc_beta.hpp>
#include <stan/math/prim/scal/fun/inc_beta.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <boost/random/student_t_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * The log of the Student-t density for the given y, nu, mean, and
 * scale parameter.  The scale parameter must be greater
 * than 0.
 *
 * \f{eqnarray*}{
 y &\sim& t_{\nu} (\mu, \sigma^2) \\
 \log (p (y \, |\, \nu, \mu, \sigma) ) &=& \log \left( \frac{\Gamma((\nu + 1)
 /2)}
 {\Gamma(\nu/2)\sqrt{\nu \pi} \sigma} \left( 1 + \frac{1}{\nu} (\frac{y -
 \mu}{\sigma})^2 \right)^{-(\nu + 1)/2} \right) \\
 &=& \log( \Gamma( (\nu+1)/2 )) - \log (\Gamma (\nu/2) - \frac{1}{2} \log(\nu
 \pi) - \log(\sigma)
 -\frac{\nu + 1}{2} \log (1 + \frac{1}{\nu} (\frac{y - \mu}{\sigma})^2)
 \f}
 *
 * @param y A scalar variable.
 * @param nu Degrees of freedom.
 * @param mu The mean of the Student-t distribution.
 * @param sigma The scale parameter of the Student-t distribution.
 * @return The log of the Student-t density at y.
 * @throw std::domain_error if sigma is not greater than 0.
 * @throw std::domain_error if nu is not greater than 0.
 * @tparam T_y Type of scalar.
 * @tparam T_dof Type of degrees of freedom.
 * @tparam T_loc Type of location.
 * @tparam T_scale Type of scale.
 */
template <bool propto, typename T_y, typename T_dof, typename T_loc,
          typename T_scale>
typename return_type<T_y, T_dof, T_loc, T_scale>::type student_t_lpdf(
    const T_y& y, const T_dof& nu, const T_loc& mu, const T_scale& sigma) {
  static const char* function = "student_t_lpdf";
  typedef typename stan::partials_return_type<T_y, T_dof, T_loc, T_scale>::type
      T_partials_return;

  if (size_zero(y, nu, mu, sigma))
    return 0.0;

  T_partials_return logp(0.0);

  check_not_nan(function, "Random variable", y);
  check_positive_finite(function, "Degrees of freedom parameter", nu);
  check_finite(function, "Location parameter", mu);
  check_positive_finite(function, "Scale parameter", sigma);
  check_consistent_sizes(function, "Random variable", y,
                         "Degrees of freedom parameter", nu,
                         "Location parameter", mu, "Scale parameter", sigma);

  if (!include_summand<propto, T_y, T_dof, T_loc, T_scale>::value)
    return 0.0;

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_dof> nu_vec(nu);
  scalar_seq_view<T_loc> mu_vec(mu);
  scalar_seq_view<T_scale> sigma_vec(sigma);
  size_t N = max_size(y, nu, mu, sigma);

  using std::log;
  using std::log;

  VectorBuilder<include_summand<propto, T_y, T_dof, T_loc, T_scale>::value,
                T_partials_return, T_dof>
      half_nu(length(nu));
  for (size_t i = 0; i < length(nu); i++)
    if (include_summand<propto, T_y, T_dof, T_loc, T_scale>::value)
      half_nu[i] = 0.5 * value_of(nu_vec[i]);

  VectorBuilder<include_summand<propto, T_dof>::value, T_partials_return, T_dof>
      lgamma_half_nu(length(nu));
  VectorBuilder<include_summand<propto, T_dof>::value, T_partials_return, T_dof>
      lgamma_half_nu_plus_half(length(nu));
  if (include_summand<propto, T_dof>::value) {
    for (size_t i = 0; i < length(nu); i++) {
      lgamma_half_nu[i] = lgamma(half_nu[i]);
      lgamma_half_nu_plus_half[i] = lgamma(half_nu[i] + 0.5);
    }
  }

  VectorBuilder<!is_constant_struct<T_dof>::value, T_partials_return, T_dof>
      digamma_half_nu(length(nu));
  VectorBuilder<!is_constant_struct<T_dof>::value, T_partials_return, T_dof>
      digamma_half_nu_plus_half(length(nu));
  if (!is_constant_struct<T_dof>::value) {
    for (size_t i = 0; i < length(nu); i++) {
      digamma_half_nu[i] = digamma(half_nu[i]);
      digamma_half_nu_plus_half[i] = digamma(half_nu[i] + 0.5);
    }
  }

  VectorBuilder<include_summand<propto, T_dof>::value, T_partials_return, T_dof>
      log_nu(length(nu));
  for (size_t i = 0; i < length(nu); i++)
    if (include_summand<propto, T_dof>::value)
      log_nu[i] = log(value_of(nu_vec[i]));

  VectorBuilder<include_summand<propto, T_scale>::value, T_partials_return,
                T_scale>
      log_sigma(length(sigma));
  for (size_t i = 0; i < length(sigma); i++)
    if (include_summand<propto, T_scale>::value)
      log_sigma[i] = log(value_of(sigma_vec[i]));

  VectorBuilder<include_summand<propto, T_y, T_dof, T_loc, T_scale>::value,
                T_partials_return, T_y, T_dof, T_loc, T_scale>
      square_y_minus_mu_over_sigma__over_nu(N);

  VectorBuilder<include_summand<propto, T_y, T_dof, T_loc, T_scale>::value,
                T_partials_return, T_y, T_dof, T_loc, T_scale>
      log1p_exp(N);

  for (size_t i = 0; i < N; i++)
    if (include_summand<propto, T_y, T_dof, T_loc, T_scale>::value) {
      const T_partials_return y_dbl = value_of(y_vec[i]);
      const T_partials_return mu_dbl = value_of(mu_vec[i]);
      const T_partials_return sigma_dbl = value_of(sigma_vec[i]);
      const T_partials_return nu_dbl = value_of(nu_vec[i]);
      square_y_minus_mu_over_sigma__over_nu[i]
          = square((y_dbl - mu_dbl) / sigma_dbl) / nu_dbl;
      log1p_exp[i] = log1p(square_y_minus_mu_over_sigma__over_nu[i]);
    }

  operands_and_partials<T_y, T_dof, T_loc, T_scale> ops_partials(y, nu, mu,
                                                                 sigma);
  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return mu_dbl = value_of(mu_vec[n]);
    const T_partials_return sigma_dbl = value_of(sigma_vec[n]);
    const T_partials_return nu_dbl = value_of(nu_vec[n]);
    if (include_summand<propto>::value)
      logp += NEG_LOG_SQRT_PI;
    if (include_summand<propto, T_dof>::value)
      logp += lgamma_half_nu_plus_half[n] - lgamma_half_nu[n] - 0.5 * log_nu[n];
    if (include_summand<propto, T_scale>::value)
      logp -= log_sigma[n];
    if (include_summand<propto, T_y, T_dof, T_loc, T_scale>::value)
      logp -= (half_nu[n] + 0.5) * log1p_exp[n];

    if (!is_constant_struct<T_y>::value) {
      ops_partials.edge1_.partials_[n]
          += -(half_nu[n] + 0.5) * 1.0
             / (1.0 + square_y_minus_mu_over_sigma__over_nu[n])
             * (2.0 * (y_dbl - mu_dbl) / square(sigma_dbl) / nu_dbl);
    }
    if (!is_constant_struct<T_dof>::value) {
      const T_partials_return inv_nu = 1.0 / nu_dbl;
      ops_partials.edge2_.partials_[n]
          += 0.5 * digamma_half_nu_plus_half[n] - 0.5 * digamma_half_nu[n]
             - 0.5 * inv_nu - 0.5 * log1p_exp[n]
             + (half_nu[n] + 0.5)
                   * (1.0 / (1.0 + square_y_minus_mu_over_sigma__over_nu[n])
                      * square_y_minus_mu_over_sigma__over_nu[n] * inv_nu);
    }
    if (!is_constant_struct<T_loc>::value) {
      ops_partials.edge3_.partials_[n]
          -= (half_nu[n] + 0.5)
             / (1.0 + square_y_minus_mu_over_sigma__over_nu[n])
             * (2.0 * (mu_dbl - y_dbl) / (sigma_dbl * sigma_dbl * nu_dbl));
    }
    if (!is_constant_struct<T_scale>::value) {
      const T_partials_return inv_sigma = 1.0 / sigma_dbl;
      ops_partials.edge4_.partials_[n]
          += -inv_sigma
             + (nu_dbl + 1.0) / (1.0 + square_y_minus_mu_over_sigma__over_nu[n])
                   * (square_y_minus_mu_over_sigma__over_nu[n] * inv_sigma);
    }
  }
  return ops_partials.build(logp);
}

template <typename T_y, typename T_dof, typename T_loc, typename T_scale>
inline typename return_type<T_y, T_dof, T_loc, T_scale>::type student_t_lpdf(
    const T_y& y, const T_dof& nu, const T_loc& mu, const T_scale& sigma) {
  return student_t_lpdf<false>(y, nu, mu, sigma);
}

}  // namespace math
}  // namespace stan
#endif
