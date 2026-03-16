#ifndef STAN_MATH_PRIM_SCAL_PROB_INV_GAMMA_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_INV_GAMMA_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/inv_gamma_lpdf.hpp>

namespace stan {
namespace math {

/**
 * The log of an inverse gamma density for y with the specified
 * shape and scale parameters.
 * Shape and scale parameters must be greater than 0.
 * y must be greater than 0.
 *
 * @deprecated use <code>inv_gamma_lpdf</code>
 *
 * @param y A scalar variable.
 * @param alpha Shape parameter.
 * @param beta Scale parameter.
 * @throw std::domain_error if alpha is not greater than 0.
 * @throw std::domain_error if beta is not greater than 0.
 * @throw std::domain_error if y is not greater than 0.
 * @tparam T_y Type of scalar.
 * @tparam T_shape Type of shape.
 * @tparam T_scale Type of scale.
 */
template <bool propto, typename T_y, typename T_shape, typename T_scale>
typename return_type<T_y, T_shape, T_scale>::type inv_gamma_log(
    const T_y& y, const T_shape& alpha, const T_scale& beta) {
  return inv_gamma_lpdf<propto, T_y, T_shape, T_scale>(y, alpha, beta);
}

/**
 * @deprecated use <code>inv_gamma_lpdf</code>
 */
template <typename T_y, typename T_shape, typename T_scale>
inline typename return_type<T_y, T_shape, T_scale>::type inv_gamma_log(
    const T_y& y, const T_shape& alpha, const T_scale& beta) {
  return inv_gamma_lpdf<T_y, T_shape, T_scale>(y, alpha, beta);
}

}  // namespace math
}  // namespace stan
#endif
