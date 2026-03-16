#ifndef STAN_MATH_PRIM_SCAL_PROB_INV_GAMMA_CCDF_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_INV_GAMMA_CCDF_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/inv_gamma_lccdf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>inv_gamma_lccdf</code>
 */
template <typename T_y, typename T_shape, typename T_scale>
typename return_type<T_y, T_shape, T_scale>::type inv_gamma_ccdf_log(
    const T_y& y, const T_shape& alpha, const T_scale& beta) {
  return inv_gamma_lccdf<T_y, T_shape, T_scale>(y, alpha, beta);
}

}  // namespace math
}  // namespace stan
#endif
