#ifndef STAN_MATH_PRIM_SCAL_PROB_EXPONENTIAL_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_EXPONENTIAL_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/exponential_lpdf.hpp>

namespace stan {
namespace math {

/**
 * The log of an exponential density for y with the specified
 * inverse scale parameter.
 * Inverse scale parameter must be greater than 0.
 * y must be greater than or equal to 0.
 *
 \f{eqnarray*}{
 y
 &\sim&
 \mbox{\sf{Expon}}(\beta) \\
 \log (p (y \, |\, \beta) )
 &=&
 \log \left( \beta \exp^{-\beta y} \right) \\
 &=&
 \log (\beta) - \beta y \\
 & &
 \mathrm{where} \; y > 0
 \f}
 *
 * @deprecated use <code>exponential_lpdf</code>
 *
 * @param y A scalar variable.
 * @param beta Inverse scale parameter.
 * @throw std::domain_error if beta is not greater than 0.
 * @throw std::domain_error if y is not greater than or equal to 0.
 * @tparam T_y Type of scalar.
 * @tparam T_inv_scale Type of inverse scale.
 */
template <bool propto, typename T_y, typename T_inv_scale>
typename return_type<T_y, T_inv_scale>::type exponential_log(
    const T_y& y, const T_inv_scale& beta) {
  return exponential_lpdf<propto, T_y, T_inv_scale>(y, beta);
}

/**
 * @deprecated use <code>exponential_lpdf</code>
 */
template <typename T_y, typename T_inv_scale>
inline typename return_type<T_y, T_inv_scale>::type exponential_log(
    const T_y& y, const T_inv_scale& beta) {
  return exponential_lpdf<T_y, T_inv_scale>(y, beta);
}

}  // namespace math
}  // namespace stan
#endif
