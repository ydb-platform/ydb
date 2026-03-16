#ifndef STAN_MATH_PRIM_SCAL_PROB_BETA_PROPORTION_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BETA_PROPORTION_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/beta_proportion_lpdf.hpp>

namespace stan {
namespace math {

/**
 * The log of the beta density for specified y, location, and
 * precision: beta_proportion_lpdf(y | mu, kappa) = beta_lpdf(y | mu *
 * kappa, (1 - mu) * kappa).  Any arguments other than scalars must be
 * containers of the same size.  With non-scalar arguments, the return
 * is the sum of the log pdfs with scalars broadcast as necessary.
 *
 * <p> The result log probability is defined to be the sum of
 * the log probabilities for each observation/mu/kappa triple.
 *
 * Prior location, mu, must be contained in (0, 1).  Prior precision
 * must be positive.
 *
 * @deprecated use <code>beta_proportion_lpdf</code>
 *
 * @param y (Sequence of) scalar(s) between zero and one
 * @param mu (Sequence of) location parameter(s)
 * @param kappa (Sequence of) precision parameter(s)
 * @return The log of the product of densities.
 * @tparam T_y Type of scalar outcome.
 * @tparam T_loc Type of prior location.
 * @tparam T_prec Type of prior precision.
 */
template <bool propto, typename T_y, typename T_loc, typename T_prec>
typename return_type<T_y, T_loc, T_prec>::type beta_proportion_log(
    const T_y& y, const T_loc& mu, const T_prec& kappa) {
  return beta_proportion_lpdf<propto, T_y, T_loc, T_prec>(y, mu, kappa);
}

/**
 * @deprecated use <code>beta_proportion_lpdf</code>
 */
template <typename T_y, typename T_loc, typename T_prec>
inline typename return_type<T_y, T_loc, T_prec>::type beta_proportion_log(
    const T_y& y, const T_loc& mu, const T_prec& kappa) {
  return beta_proportion_lpdf<T_y, T_loc, T_prec>(y, mu, kappa);
}

}  // namespace math
}  // namespace stan
#endif
