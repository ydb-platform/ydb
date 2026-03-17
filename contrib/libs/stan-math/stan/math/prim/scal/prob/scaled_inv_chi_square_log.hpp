#ifndef STAN_MATH_PRIM_SCAL_PROB_SCALED_INV_CHI_SQUARE_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_SCALED_INV_CHI_SQUARE_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/scaled_inv_chi_square_lpdf.hpp>

namespace stan {
namespace math {

/**
 * The log of a scaled inverse chi-squared density for y with the
 * specified degrees of freedom parameter and scale parameter.
 *
 \f{eqnarray*}{
 y &\sim& \mbox{\sf{Inv-}}\chi^2(\nu, s^2) \\
 \log (p (y \, |\, \nu, s)) &=& \log \left( \frac{(\nu / 2)^{\nu / 2}}{\Gamma
 (\nu / 2)} s^\nu y^{- (\nu / 2 + 1)} \exp^{-\nu s^2 / (2y)} \right) \\
 &=& \frac{\nu}{2} \log(\frac{\nu}{2}) - \log (\Gamma (\nu / 2)) + \nu \log(s) -
 (\frac{\nu}{2} + 1) \log(y) - \frac{\nu s^2}{2y} \\ & & \mathrm{ where } \; y >
 0 \f}
 *
 * @deprecated use <code>scaled_inv_chi_square_lpdf</code>
 *
 * @param y A scalar variable.
 * @param nu Degrees of freedom.
 * @param s Scale parameter.
 * @throw std::domain_error if nu is not greater than 0
 * @throw std::domain_error if s is not greater than 0.
 * @throw std::domain_error if y is not greater than 0.
 * @tparam T_y Type of scalar.
 * @tparam T_dof Type of degrees of freedom.
 */
template <bool propto, typename T_y, typename T_dof, typename T_scale>
typename return_type<T_y, T_dof, T_scale>::type scaled_inv_chi_square_log(
    const T_y& y, const T_dof& nu, const T_scale& s) {
  return scaled_inv_chi_square_lpdf<propto, T_y, T_dof, T_scale>(y, nu, s);
}

/**
 * @deprecated use <code>scaled_inv_chi_square_lpdf</code>
 */
template <typename T_y, typename T_dof, typename T_scale>
inline typename return_type<T_y, T_dof, T_scale>::type
scaled_inv_chi_square_log(const T_y& y, const T_dof& nu, const T_scale& s) {
  return scaled_inv_chi_square_lpdf<T_y, T_dof, T_scale>(y, nu, s);
}

}  // namespace math
}  // namespace stan
#endif
