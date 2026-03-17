#ifndef STAN_MATH_PRIM_SCAL_PROB_BETA_PROPORTION_CCDF_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BETA_PROPORTION_CCDF_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/beta_proportion_lccdf.hpp>

namespace stan {
namespace math {

/**
 * Returns the beta log complementary cumulative distribution function
 * for specified probability, location, and precision parameters:
 * beta_proportion_lccdf(y | mu, kappa) = beta_lccdf(y | mu * kappa, (1 -
 * mu) * kappa).  Any arguments other than scalars must be containers of
 * the same size.  With non-scalar arguments, the return is the sum of
 * the log ccdfs with scalars broadcast as necessary.
 *
 * @deprecated use <code>beta_proportion_lccdf</code>
 *
 * @tparam T_y type of y
 * @tparam T_loc type of location parameter
 * @tparam T_prec type of precision parameter
 * @param y (Sequence of) scalar(s) between zero and one
 * @param mu (Sequence of) location parameter(s)
 * @param kappa (Sequence of) precision parameter(s)
 * @return log probability or sum of log of probabilities
 * @throw std::domain_error if mu is outside (0, 1)
 * @throw std::domain_error if kappa is nonpositive
 * @throw std::domain_error if 1 - y is not a valid probability
 * @throw std::invalid_argument if container sizes mismatch
 */
template <typename T_y, typename T_loc, typename T_prec>
typename return_type<T_y, T_loc, T_prec>::type beta_proportion_ccdf_log(
    const T_y& y, const T_loc& mu, const T_prec& kappa) {
  return beta_proportion_lccdf<T_y, T_loc, T_prec>(y, mu, kappa);
}

}  // namespace math
}  // namespace stan
#endif
