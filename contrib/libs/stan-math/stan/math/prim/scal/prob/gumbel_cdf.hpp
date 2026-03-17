#ifndef STAN_MATH_PRIM_SCAL_PROB_GUMBEL_CDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_GUMBEL_CDF_HPP

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
 * Returns the Gumbel distribution cumulative distribution for the given
 * location and scale. Given containers of matching sizes, returns the
 * product of probabilities.
 *
 * @tparam T_y type of real parameter
 * @tparam T_loc type of location parameter
 * @tparam T_scale type of scale parameter
 * @param y real parameter
 * @param mu location parameter
 * @param beta scale parameter
 * @return probability or product of probabilities
 * @throw std::domain_error if y is nan, mu is infinite, or beta is nonpositive
 * @throw std::invalid_argument if container sizes mismatch
 */
template <typename T_y, typename T_loc, typename T_scale>
typename return_type<T_y, T_loc, T_scale>::type gumbel_cdf(
    const T_y& y, const T_loc& mu, const T_scale& beta) {
  static const char* function = "gumbel_cdf";
  typedef typename stan::partials_return_type<T_y, T_loc, T_scale>::type
      T_partials_return;

  using std::exp;

  T_partials_return cdf(1.0);
  if (size_zero(y, mu, beta))
    return cdf;

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
    const T_partials_return cdf_ = exp(-exp(-scaled_diff));
    cdf *= cdf_;

    if (!is_constant_struct<T_y>::value)
      ops_partials.edge1_.partials_[n] += rep_deriv / cdf_;
    if (!is_constant_struct<T_loc>::value)
      ops_partials.edge2_.partials_[n] -= rep_deriv / cdf_;
    if (!is_constant_struct<T_scale>::value)
      ops_partials.edge3_.partials_[n] -= rep_deriv * scaled_diff / cdf_;
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
    for (size_t n = 0; n < stan::length(beta); ++n)
      ops_partials.edge3_.partials_[n] *= cdf;
  }
  return ops_partials.build(cdf);
}

}  // namespace math
}  // namespace stan
#endif
