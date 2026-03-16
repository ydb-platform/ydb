#ifndef STAN_MATH_PRIM_MAT_PROB_WIENER_LOG_HPP
#define STAN_MATH_PRIM_MAT_PROB_WIENER_LOG_HPP

#include <stan/math/prim/scal/prob/wiener_lpdf.hpp>
#include <boost/math/tools/promotion.hpp>

namespace stan {
namespace math {

/**
 * The log of the first passage time density function for a (Wiener)
 *  drift diffusion model for the given \f$y\f$,
 * boundary separation \f$\alpha\f$, nondecision time \f$\tau\f$,
 * relative bias \f$\beta\f$, and drift rate \f$\delta\f$.
 * \f$\alpha\f$ and \f$\tau\f$ must be greater than 0, and
 * \f$\beta\f$ must be between 0 and 1. \f$y\f$ should contain
 * reaction times in seconds (strictly positive) with
 * upper-boundary responses.
 *
 * @deprecated use <code>wiener_lpdf</code>
 *
 * @param y A scalar variate.
 * @param alpha The boundary separation.
 * @param tau The nondecision time.
 * @param beta The relative bias.
 * @param delta The drift rate.
 * @return The log of the Wiener first passage time density of
 *  the specified arguments.
 */
template <bool propto, typename T_y, typename T_alpha, typename T_tau,
          typename T_beta, typename T_delta>
typename return_type<T_y, T_alpha, T_tau, T_beta, T_delta>::type wiener_log(
    const T_y& y, const T_alpha& alpha, const T_tau& tau, const T_beta& beta,
    const T_delta& delta) {
  return wiener_lpdf<propto, T_y, T_alpha, T_tau, T_beta, T_delta>(
      y, alpha, tau, beta, delta);
}

/**
 * @deprecated use <code>wiener_lpdf</code>
 */
template <typename T_y, typename T_alpha, typename T_tau, typename T_beta,
          typename T_delta>
inline typename return_type<T_y, T_alpha, T_tau, T_beta, T_delta>::type
wiener_log(const T_y& y, const T_alpha& alpha, const T_tau& tau,
           const T_beta& beta, const T_delta& delta) {
  return wiener_lpdf<T_y, T_alpha, T_tau, T_beta, T_delta>(y, alpha, tau, beta,
                                                           delta);
}

}  // namespace math
}  // namespace stan
#endif
