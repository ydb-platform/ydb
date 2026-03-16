#ifndef STAN_MATH_PRIM_SCAL_PROB_NORMAL_SUFFICIENT_LPDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_NORMAL_SUFFICIENT_LPDF_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/normal_lpdf.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/meta/max_size.hpp>

namespace stan {
namespace math {

/**
 * The log of the normal density for the specified scalar(s) given
 * the specified mean(s) and deviation(s).
 * y, s_quared, mu, or sigma can each be either
 * a scalar, a std vector or Eigen vector.
 * n can be either a single int or an std vector of ints.
 * Any vector inputs must be the same length.
 *
 * <p>The result log probability is defined to be the sum of the
 * log probabilities for each observation/mean/deviation triple.
 *
 * @tparam T_y Type of sample average parameter.
 * @tparam T_s Type of sample squared errors parameter.
 * @tparam T_n Type of sample size parameter.
 * @tparam T_loc Type of location parameter.
 * @tparam T_scale Type of scale parameter.
 * @param y_bar (Sequence of) scalar(s) (sample average(s)).
 * @param s_squared (Sequence of) sum(s) of sample squared errors
 * @param n_obs (Sequence of) sample size(s)
 * @param mu (Sequence of) location parameter(s)
 * for the normal distribution.
 * @param sigma (Sequence of) scale parameters for the normal
 * distribution.
 * @return The log of the product of the densities.
 * @throw std::domain_error if either n or sigma are not positive,
 * if s_squared is negative or if any parameter is not finite.
 */
template <bool propto, typename T_y, typename T_s, typename T_n, typename T_loc,
          typename T_scale>
typename return_type<T_y, T_s, T_loc, T_scale>::type normal_sufficient_lpdf(
    const T_y& y_bar, const T_s& s_squared, const T_n& n_obs, const T_loc& mu,
    const T_scale& sigma) {
  static const char* function = "normal_sufficient_lpdf";
  typedef
      typename stan::partials_return_type<T_y, T_s, T_n, T_loc, T_scale>::type
          T_partials_return;

  using std::log;

  // check if any vectors are zero length
  if (size_zero(y_bar, s_squared, n_obs, mu, sigma))
    return 0.0;

  // set up return value accumulator
  T_partials_return logp(0.0);

  // validate args (here done over var, which should be OK)
  check_finite(function, "Location parameter sufficient statistic", y_bar);
  check_finite(function, "Scale parameter sufficient statistic", s_squared);
  check_nonnegative(function, "Scale parameter sufficient statistic",
                    s_squared);
  check_finite(function, "Number of observations", n_obs);
  check_positive(function, "Number of observations", n_obs);
  check_finite(function, "Location parameter", mu);
  check_finite(function, "Scale parameter", sigma);
  check_positive(function, "Scale parameter", sigma);
  check_consistent_sizes(function, "Location parameter sufficient statistic",
                         y_bar, "Scale parameter sufficient statistic",
                         s_squared, "Number of observations", n_obs,
                         "Location parameter", mu, "Scale parameter", sigma);
  // check if no variables are involved and prop-to
  if (!include_summand<propto, T_y, T_s, T_loc, T_scale>::value)
    return 0.0;

  // set up template expressions wrapping scalars into vector views
  operands_and_partials<T_y, T_s, T_loc, T_scale> ops_partials(y_bar, s_squared,
                                                               mu, sigma);

  scalar_seq_view<const T_y> y_bar_vec(y_bar);
  scalar_seq_view<const T_s> s_squared_vec(s_squared);
  scalar_seq_view<const T_n> n_obs_vec(n_obs);
  scalar_seq_view<const T_loc> mu_vec(mu);
  scalar_seq_view<const T_scale> sigma_vec(sigma);
  size_t N = max_size(y_bar, s_squared, n_obs, mu, sigma);

  for (size_t i = 0; i < N; i++) {
    const T_partials_return y_bar_dbl = value_of(y_bar_vec[i]);
    const T_partials_return s_squared_dbl = value_of(s_squared_vec[i]);
    const T_partials_return n_obs_dbl = n_obs_vec[i];
    const T_partials_return mu_dbl = value_of(mu_vec[i]);
    const T_partials_return sigma_dbl = value_of(sigma_vec[i]);
    const T_partials_return sigma_squared = pow(sigma_dbl, 2);

    if (include_summand<propto>::value)
      logp += NEG_LOG_SQRT_TWO_PI * n_obs_dbl;

    if (include_summand<propto, T_scale>::value)
      logp -= n_obs_dbl * log(sigma_dbl);

    const T_partials_return cons_expr
        = (s_squared_dbl + n_obs_dbl * pow(y_bar_dbl - mu_dbl, 2));

    logp -= cons_expr / (2 * sigma_squared);

    // gradients
    if (!is_constant_struct<T_y>::value || !is_constant_struct<T_loc>::value) {
      const T_partials_return common_derivative
          = n_obs_dbl * (mu_dbl - y_bar_dbl) / sigma_squared;
      if (!is_constant_struct<T_y>::value)
        ops_partials.edge1_.partials_[i] += common_derivative;
      if (!is_constant_struct<T_loc>::value)
        ops_partials.edge3_.partials_[i] -= common_derivative;
    }
    if (!is_constant_struct<T_s>::value)
      ops_partials.edge2_.partials_[i] -= 0.5 / sigma_squared;
    if (!is_constant_struct<T_scale>::value)
      ops_partials.edge4_.partials_[i]
          += cons_expr / pow(sigma_dbl, 3) - n_obs_dbl / sigma_dbl;
  }
  return ops_partials.build(logp);
}

template <typename T_y, typename T_s, typename T_n, typename T_loc,
          typename T_scale>
inline typename return_type<T_y, T_s, T_loc, T_scale>::type
normal_sufficient_lpdf(const T_y& y_bar, const T_s& s_squared, const T_n& n_obs,
                       const T_loc& mu, const T_scale& sigma) {
  return normal_sufficient_lpdf<false>(y_bar, s_squared, n_obs, mu, sigma);
}

}  // namespace math
}  // namespace stan
#endif
