#ifndef STAN_MATH_PRIM_SCAL_PROB_CAUCHY_CDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_CAUCHY_CDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/log1p.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <boost/random/cauchy_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <limits>

namespace stan {
namespace math {

/**
 * Returns the cauchy cumulative distribution function for the given
 * location, and scale. If given containers of matching sizes
 * returns the product of probabilities.
 *
 * @tparam T_y type of real parameter
 * @tparam T_loc type of location parameter
 * @tparam T_scale type of scale parameter
 * @param y real parameter
 * @param mu location parameter
 * @param sigma scale parameter
 * @return probability or product of probabilities
 * @throw std::domain_error if sigma is nonpositive or y, mu are nan
 * @throw std::invalid_argument if container sizes mismatch
 */
template <typename T_y, typename T_loc, typename T_scale>
typename return_type<T_y, T_loc, T_scale>::type cauchy_cdf(
    const T_y& y, const T_loc& mu, const T_scale& sigma) {
  typedef typename stan::partials_return_type<T_y, T_loc, T_scale>::type
      T_partials_return;

  if (size_zero(y, mu, sigma))
    return 1.0;

  static const char* function = "cauchy_cdf";

  using boost::math::tools::promote_args;

  T_partials_return P(1.0);

  check_not_nan(function, "Random variable", y);
  check_finite(function, "Location parameter", mu);
  check_positive_finite(function, "Scale parameter", sigma);
  check_consistent_sizes(function, "Random variable", y, "Location parameter",
                         mu, "Scale Parameter", sigma);

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_loc> mu_vec(mu);
  scalar_seq_view<T_scale> sigma_vec(sigma);
  size_t N = max_size(y, mu, sigma);

  operands_and_partials<T_y, T_loc, T_scale> ops_partials(y, mu, sigma);

  // Explicit return for extreme values
  // The gradients are technically ill-defined, but treated as zero
  for (size_t i = 0; i < stan::length(y); i++) {
    if (value_of(y_vec[i]) == -std::numeric_limits<double>::infinity())
      return ops_partials.build(0.0);
  }

  using std::atan;

  for (size_t n = 0; n < N; n++) {
    // Explicit results for extreme values
    // The gradients are technically ill-defined, but treated as zero
    if (value_of(y_vec[n]) == std::numeric_limits<double>::infinity()) {
      continue;
    }

    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return mu_dbl = value_of(mu_vec[n]);
    const T_partials_return sigma_inv_dbl = 1.0 / value_of(sigma_vec[n]);

    const T_partials_return z = (y_dbl - mu_dbl) * sigma_inv_dbl;

    const T_partials_return Pn = atan(z) / pi() + 0.5;

    P *= Pn;

    if (!is_constant_struct<T_y>::value)
      ops_partials.edge1_.partials_[n]
          += sigma_inv_dbl / (pi() * (1.0 + z * z) * Pn);
    if (!is_constant_struct<T_loc>::value)
      ops_partials.edge2_.partials_[n]
          += -sigma_inv_dbl / (pi() * (1.0 + z * z) * Pn);
    if (!is_constant_struct<T_scale>::value)
      ops_partials.edge3_.partials_[n]
          += -z * sigma_inv_dbl / (pi() * (1.0 + z * z) * Pn);
  }

  if (!is_constant_struct<T_y>::value) {
    for (size_t n = 0; n < stan::length(y); ++n)
      ops_partials.edge1_.partials_[n] *= P;
  }
  if (!is_constant_struct<T_loc>::value) {
    for (size_t n = 0; n < stan::length(mu); ++n)
      ops_partials.edge2_.partials_[n] *= P;
  }
  if (!is_constant_struct<T_scale>::value) {
    for (size_t n = 0; n < stan::length(sigma); ++n)
      ops_partials.edge3_.partials_[n] *= P;
  }
  return ops_partials.build(P);
}

}  // namespace math
}  // namespace stan
#endif
