#ifndef STAN_MATH_PRIM_SCAL_PROB_STUDENT_T_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_STUDENT_T_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/student_t_lpdf.hpp>

namespace stan {
namespace math {

/**
 * The log of the Student-t density for the given y, nu, mean, and
 * scale parameter.  The scale parameter must be greater
 * than 0.
 *
 * \f{eqnarray*}{
 y &\sim& t_{\nu} (\mu, \sigma^2) \\
 \log (p (y \, |\, \nu, \mu, \sigma) ) &=& \log \left( \frac{\Gamma((\nu + 1)
 /2)}
 {\Gamma(\nu/2)\sqrt{\nu \pi} \sigma} \left( 1 + \frac{1}{\nu} (\frac{y -
 \mu}{\sigma})^2 \right)^{-(\nu + 1)/2} \right) \\
 &=& \log( \Gamma( (\nu+1)/2 )) - \log (\Gamma (\nu/2) - \frac{1}{2} \log(\nu
 \pi) - \log(\sigma)
 -\frac{\nu + 1}{2} \log (1 + \frac{1}{\nu} (\frac{y - \mu}{\sigma})^2)
 \f}
 *
 * @deprecated use <code>student_t_lpdf</code>
 *
 * @param y A scalar variable.
 * @param nu Degrees of freedom.
 * @param mu The mean of the Student-t distribution.
 * @param sigma The scale parameter of the Student-t distribution.
 * @return The log of the Student-t density at y.
 * @throw std::domain_error if sigma is not greater than 0.
 * @throw std::domain_error if nu is not greater than 0.
 * @tparam T_y Type of scalar.
 * @tparam T_dof Type of degrees of freedom.
 * @tparam T_loc Type of location.
 * @tparam T_scale Type of scale.
 */
template <bool propto, typename T_y, typename T_dof, typename T_loc,
          typename T_scale>
typename return_type<T_y, T_dof, T_loc, T_scale>::type student_t_log(
    const T_y& y, const T_dof& nu, const T_loc& mu, const T_scale& sigma) {
  return student_t_lpdf<propto, T_y, T_dof, T_loc, T_scale>(y, nu, mu, sigma);
}

/**
 * @deprecated use <code>student_t_lpdf</code>
 */
template <typename T_y, typename T_dof, typename T_loc, typename T_scale>
inline typename return_type<T_y, T_dof, T_loc, T_scale>::type student_t_log(
    const T_y& y, const T_dof& nu, const T_loc& mu, const T_scale& sigma) {
  return student_t_lpdf<T_y, T_dof, T_loc, T_scale>(y, nu, mu, sigma);
}

}  // namespace math
}  // namespace stan
#endif
