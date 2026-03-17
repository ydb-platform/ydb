#ifndef STAN_MATH_PRIM_SCAL_PROB_GUMBEL_LCCDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_GUMBEL_LCCDF_HPP

#include <boost/random/uniform_01.hpp>
#include <boost/random/variate_generator.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/meta/length.hpp>
#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Returns the Gumbel log complementary cumulative distribution for the
 * given location and scale. Given containers of matching sizes, returns
 * the log sum of probabilities.
 *
 * @tparam T_y type of real parameter
 * @tparam T_loc type of location parameter
 * @tparam T_scale type of scale parameter
 * @param y real parameter
 * @param mu location parameter
 * @param beta scale parameter
 * @return log probability or log sum of probabilities
 * @throw std::domain_error if y is nan, mu is infinite, or beta is nonpositive
 * @throw std::invalid_argument if container sizes mismatch
 */
template <typename T_y, typename T_loc, typename T_scale>
typename return_type<T_y, T_loc, T_scale>::type gumbel_lccdf(
    const T_y& y, const T_loc& mu, const T_scale& beta) {
  static const char* function = "gumbel_lccdf";
  typedef typename stan::partials_return_type<T_y, T_loc, T_scale>::type
      T_partials_return;

  using std::exp;
  using std::log;

  T_partials_return ccdf_log(0.0);
  if (size_zero(y, mu, beta))
    return ccdf_log;

  check_not_nan(function, "Random variable", y);
  check_finite(function, "Location parameter", mu);
  check_not_nan(function, "Scale parameter", beta);
  check_positive(function, "Scale parameter", beta);
  check_consistent_sizes(function, "Random variable", y, "Location parameter",
                         mu, "Scale parameter", beta);

  operands_and_partials<T_y, T_loc, T_scale> ops_partials(y, mu, beta);

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_loc> mu_vec(mu);
  scalar_seq_view<T_scale> beta_vec(beta);
  size_t N = max_size(y, mu, beta);

  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return mu_dbl = value_of(mu_vec[n]);
    const T_partials_return beta_dbl = value_of(beta_vec[n]);
    const T_partials_return scaled_diff = (y_dbl - mu_dbl) / beta_dbl;
    const T_partials_return rep_deriv
        = exp(-scaled_diff - exp(-scaled_diff)) / beta_dbl;
    const T_partials_return ccdf_log_ = 1.0 - exp(-exp(-scaled_diff));
    ccdf_log += log(ccdf_log_);

    if (!is_constant_struct<T_y>::value)
      ops_partials.edge1_.partials_[n] -= rep_deriv / ccdf_log_;
    if (!is_constant_struct<T_loc>::value)
      ops_partials.edge2_.partials_[n] += rep_deriv / ccdf_log_;
    if (!is_constant_struct<T_scale>::value)
      ops_partials.edge3_.partials_[n] += rep_deriv * scaled_diff / ccdf_log_;
  }

  return ops_partials.build(ccdf_log);
}

}  // namespace math
}  // namespace stan
#endif
