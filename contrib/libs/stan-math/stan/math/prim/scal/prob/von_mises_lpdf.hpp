#ifndef STAN_MATH_PRIM_SCAL_PROB_VON_MISES_LPDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_VON_MISES_LPDF_HPP

#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/scal/err/check_greater.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/fun/log_modified_bessel_first_kind.hpp>
#include <stan/math/prim/scal/fun/modified_bessel_first_kind.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <cmath>

namespace stan {
namespace math {

template <bool propto, typename T_y, typename T_loc, typename T_scale>
typename return_type<T_y, T_loc, T_scale>::type von_mises_lpdf(
    T_y const& y, T_loc const& mu, T_scale const& kappa) {
  static char const* const function = "von_mises_lpdf";
  typedef typename stan::partials_return_type<T_y, T_loc, T_scale>::type
      T_partials_return;

  if (size_zero(y, mu, kappa))
    return 0.0;

  using std::log;

  T_partials_return logp = 0.0;

  check_finite(function, "Random variable", y);
  check_finite(function, "Location paramter", mu);
  check_positive_finite(function, "Scale parameter", kappa);
  check_consistent_sizes(function, "Random variable", y, "Location parameter",
                         mu, "Scale parameter", kappa);

  if (!include_summand<propto, T_y, T_loc, T_scale>::value)
    return logp;

  const bool y_const = is_constant_struct<T_y>::value;
  const bool mu_const = is_constant_struct<T_loc>::value;
  const bool kappa_const = is_constant_struct<T_scale>::value;

  const bool compute_bessel0 = include_summand<propto, T_scale>::value;
  const bool compute_bessel1 = !kappa_const;
  const double TWO_PI = 2.0 * pi();

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_loc> mu_vec(mu);
  scalar_seq_view<T_scale> kappa_vec(kappa);

  VectorBuilder<true, T_partials_return, T_scale> kappa_dbl(length(kappa));
  VectorBuilder<include_summand<propto, T_scale>::value, T_partials_return,
                T_scale>
      log_bessel0(length(kappa));
  for (size_t i = 0; i < length(kappa); i++) {
    kappa_dbl[i] = value_of(kappa_vec[i]);
    if (include_summand<propto, T_scale>::value)
      log_bessel0[i]
          = log_modified_bessel_first_kind(0, value_of(kappa_vec[i]));
  }

  operands_and_partials<T_y, T_loc, T_scale> ops_partials(y, mu, kappa);

  size_t N = max_size(y, mu, kappa);

  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_ = value_of(y_vec[n]);
    const T_partials_return y_dbl = y_ - floor(y_ / TWO_PI) * TWO_PI;
    const T_partials_return mu_dbl = value_of(mu_vec[n]);

    T_partials_return bessel0 = 0;
    if (compute_bessel0)
      bessel0 = modified_bessel_first_kind(0, kappa_dbl[n]);
    T_partials_return bessel1 = 0;
    if (compute_bessel1)
      bessel1 = modified_bessel_first_kind(-1, kappa_dbl[n]);
    const T_partials_return kappa_sin = kappa_dbl[n] * sin(mu_dbl - y_dbl);
    const T_partials_return kappa_cos = kappa_dbl[n] * cos(mu_dbl - y_dbl);

    if (include_summand<propto>::value)
      logp -= LOG_TWO_PI;
    if (include_summand<propto, T_scale>::value)
      logp -= log_bessel0[n];
    if (include_summand<propto, T_y, T_loc, T_scale>::value)
      logp += kappa_cos;

    if (!y_const)
      ops_partials.edge1_.partials_[n] += kappa_sin;
    if (!mu_const)
      ops_partials.edge2_.partials_[n] -= kappa_sin;
    if (!kappa_const)
      ops_partials.edge3_.partials_[n]
          += kappa_cos / kappa_dbl[n] - bessel1 / bessel0;
  }
  return ops_partials.build(logp);
}

template <typename T_y, typename T_loc, typename T_scale>
inline typename return_type<T_y, T_loc, T_scale>::type von_mises_lpdf(
    T_y const& y, T_loc const& mu, T_scale const& kappa) {
  return von_mises_lpdf<false>(y, mu, kappa);
}

}  // namespace math
}  // namespace stan
#endif
