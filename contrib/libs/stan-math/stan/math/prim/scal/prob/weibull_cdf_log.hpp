#ifndef STAN_MATH_PRIM_SCAL_PROB_WEIBULL_CDF_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_WEIBULL_CDF_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/weibull_lcdf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>weibull_lcdf</code>
 */
template <typename T_y, typename T_shape, typename T_scale>
typename return_type<T_y, T_shape, T_scale>::type weibull_cdf_log(
    const T_y& y, const T_shape& alpha, const T_scale& sigma) {
  return weibull_lcdf<T_y, T_shape, T_scale>(y, alpha, sigma);
}

}  // namespace math
}  // namespace stan
#endif
