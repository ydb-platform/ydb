#ifndef STAN_MATH_PRIM_SCAL_PROB_CHI_SQUARE_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_CHI_SQUARE_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/chi_square_lpdf.hpp>

namespace stan {
namespace math {

/**
 * The log of a chi-squared density for y with the specified
 * degrees of freedom parameter.
 * The degrees of freedom prarameter must be greater than 0.
 * y must be greater than or equal to 0.
 *
 \f{eqnarray*}{
 y &\sim& \chi^2_\nu \\
 \log (p (y \, |\, \nu)) &=& \log \left( \frac{2^{-\nu / 2}}{\Gamma (\nu / 2)}
 y^{\nu / 2 - 1} \exp^{- y / 2} \right) \\
 &=& - \frac{\nu}{2} \log(2) - \log (\Gamma (\nu / 2)) + (\frac{\nu}{2} - 1)
 \log(y) - \frac{y}{2} \\ & & \mathrm{ where } \; y \ge 0 \f}
 *
 * @deprecated use <code>chi_square_lpdf</code>
 * @param y A scalar variable.
 * @param nu Degrees of freedom.
 * @throw std::domain_error if nu is not greater than or equal to 0
 * @throw std::domain_error if y is not greater than or equal to 0.
 * @tparam T_y Type of scalar.
 * @tparam T_dof Type of degrees of freedom.
 */
template <bool propto, typename T_y, typename T_dof>
typename return_type<T_y, T_dof>::type chi_square_log(const T_y& y,
                                                      const T_dof& nu) {
  return chi_square_lpdf<propto, T_y, T_dof>(y, nu);
}

/**
 * @deprecated use <code>chi_square_lpdf</code>
 */
template <typename T_y, typename T_dof>
inline typename return_type<T_y, T_dof>::type chi_square_log(const T_y& y,
                                                             const T_dof& nu) {
  return chi_square_lpdf<T_y, T_dof>(y, nu);
}

}  // namespace math
}  // namespace stan
#endif
