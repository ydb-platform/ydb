#ifndef STAN_MATH_PRIM_SCAL_PROB_UNIFORM_LPDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_UNIFORM_LPDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_greater.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <boost/random/uniform_real_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * The log of a uniform density for the given
 * y, lower, and upper bound.
 *
 \f{eqnarray*}{
 y &\sim& \mbox{\sf{U}}(\alpha, \beta) \\
 \log (p (y \, |\, \alpha, \beta)) &=& \log \left( \frac{1}{\beta-\alpha}
 \right) \\
 &=& \log (1) - \log (\beta - \alpha) \\
 &=& -\log (\beta - \alpha) \\
 & & \mathrm{ where } \; y \in [\alpha, \beta], \log(0) \; \mathrm{otherwise}
 \f}
 *
 * @param y A scalar variable.
 * @param alpha Lower bound.
 * @param beta Upper bound.
 * @throw std::invalid_argument if the lower bound is greater than
 *    or equal to the lower bound
 * @tparam T_y Type of scalar.
 * @tparam T_low Type of lower bound.
 * @tparam T_high Type of upper bound.
 */
template <bool propto, typename T_y, typename T_low, typename T_high>
typename return_type<T_y, T_low, T_high>::type uniform_lpdf(
    const T_y& y, const T_low& alpha, const T_high& beta) {
  static const char* function = "uniform_lpdf";
  typedef typename stan::partials_return_type<T_y, T_low, T_high>::type
      T_partials_return;

  using std::log;

  if (size_zero(y, alpha, beta))
    return 0.0;

  T_partials_return logp(0.0);
  check_not_nan(function, "Random variable", y);
  check_finite(function, "Lower bound parameter", alpha);
  check_finite(function, "Upper bound parameter", beta);
  check_greater(function, "Upper bound parameter", beta, alpha);
  check_consistent_sizes(function, "Random variable", y,
                         "Lower bound parameter", alpha,
                         "Upper bound parameter", beta);

  if (!include_summand<propto, T_y, T_low, T_high>::value)
    return 0.0;

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_low> alpha_vec(alpha);
  scalar_seq_view<T_high> beta_vec(beta);
  size_t N = max_size(y, alpha, beta);

  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_dbl = value_of(y_vec[n]);
    if (y_dbl < value_of(alpha_vec[n]) || y_dbl > value_of(beta_vec[n]))
      return LOG_ZERO;
  }

  VectorBuilder<include_summand<propto, T_low, T_high>::value,
                T_partials_return, T_low, T_high>
      inv_beta_minus_alpha(max_size(alpha, beta));
  for (size_t i = 0; i < max_size(alpha, beta); i++)
    if (include_summand<propto, T_low, T_high>::value)
      inv_beta_minus_alpha[i]
          = 1.0 / (value_of(beta_vec[i]) - value_of(alpha_vec[i]));

  VectorBuilder<include_summand<propto, T_low, T_high>::value,
                T_partials_return, T_low, T_high>
      log_beta_minus_alpha(max_size(alpha, beta));
  for (size_t i = 0; i < max_size(alpha, beta); i++)
    if (include_summand<propto, T_low, T_high>::value)
      log_beta_minus_alpha[i]
          = log(value_of(beta_vec[i]) - value_of(alpha_vec[i]));

  operands_and_partials<T_y, T_low, T_high> ops_partials(y, alpha, beta);
  for (size_t n = 0; n < N; n++) {
    if (include_summand<propto, T_low, T_high>::value)
      logp -= log_beta_minus_alpha[n];

    if (!is_constant_struct<T_low>::value)
      ops_partials.edge2_.partials_[n] += inv_beta_minus_alpha[n];
    if (!is_constant_struct<T_high>::value)
      ops_partials.edge3_.partials_[n] -= inv_beta_minus_alpha[n];
  }
  return ops_partials.build(logp);
}

template <typename T_y, typename T_low, typename T_high>
inline typename return_type<T_y, T_low, T_high>::type uniform_lpdf(
    const T_y& y, const T_low& alpha, const T_high& beta) {
  return uniform_lpdf<false>(y, alpha, beta);
}

}  // namespace math
}  // namespace stan
#endif
