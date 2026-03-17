#ifndef STAN_MATH_PRIM_SCAL_PROB_GAMMA_CDF_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_GAMMA_CDF_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/gamma_lcdf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>gamma_lcdf</code>
 */
template <typename T_y, typename T_shape, typename T_inv_scale>
typename return_type<T_y, T_shape, T_inv_scale>::type gamma_cdf_log(
    const T_y& y, const T_shape& alpha, const T_inv_scale& beta) {
  return gamma_lcdf<T_y, T_shape, T_inv_scale>(y, alpha, beta);
}

}  // namespace math
}  // namespace stan
#endif
