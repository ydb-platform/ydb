#ifndef STAN_MATH_PRIM_SCAL_PROB_GAMMA_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_GAMMA_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/gamma_lpdf.hpp>

namespace stan {
namespace math {

/**
 * The log of a gamma density for y with the specified
 * shape and inverse scale parameters.
 * Shape and inverse scale parameters must be greater than 0.
 * y must be greater than or equal to 0.
 *
 \f{eqnarray*}{
 y &\sim& \mbox{\sf{Gamma}}(\alpha, \beta) \\
 \log (p (y \, |\, \alpha, \beta) ) &=& \log \left(
 \frac{\beta^\alpha}{\Gamma(\alpha)} y^{\alpha - 1} \exp^{- \beta y} \right) \\
 &=& \alpha \log(\beta) - \log(\Gamma(\alpha)) + (\alpha - 1) \log(y) - \beta
 y\\ & & \mathrm{where} \; y > 0 \f}
 *
 * @deprecated use <code>gamma_lpdf</code>
 *
 * @param y A scalar variable.
 * @param alpha Shape parameter.
 * @param beta Inverse scale parameter.
 * @throw std::domain_error if alpha is not greater than 0.
 * @throw std::domain_error if beta is not greater than 0.
 * @throw std::domain_error if y is not greater than or equal to 0.
 * @tparam T_y Type of scalar.
 * @tparam T_shape Type of shape.
 * @tparam T_inv_scale Type of inverse scale.
 */
template <bool propto, typename T_y, typename T_shape, typename T_inv_scale>
typename return_type<T_y, T_shape, T_inv_scale>::type gamma_log(
    const T_y& y, const T_shape& alpha, const T_inv_scale& beta) {
  return gamma_lpdf<propto, T_y, T_shape, T_inv_scale>(y, alpha, beta);
}

/**
 * @deprecated use <code>gamma_lpdf</code>
 */
template <typename T_y, typename T_shape, typename T_inv_scale>
inline typename return_type<T_y, T_shape, T_inv_scale>::type gamma_log(
    const T_y& y, const T_shape& alpha, const T_inv_scale& beta) {
  return gamma_lpdf<T_y, T_shape, T_inv_scale>(y, alpha, beta);
}

}  // namespace math
}  // namespace stan
#endif
