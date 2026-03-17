#ifndef STAN_MATH_PRIM_SCAL_PROB_SKEW_NORMAL_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_SKEW_NORMAL_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/skew_normal_lpdf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>skew_normal_lpdf</code>
 */
template <bool propto, typename T_y, typename T_loc, typename T_scale,
          typename T_shape>
typename return_type<T_y, T_loc, T_scale, T_shape>::type skew_normal_log(
    const T_y& y, const T_loc& mu, const T_scale& sigma, const T_shape& alpha) {
  return skew_normal_lpdf<propto, T_y, T_loc, T_scale, T_shape>(y, mu, sigma,
                                                                alpha);
}

/**
 * @deprecated use <code>skew_normal_lpdf</code>
 */
template <typename T_y, typename T_loc, typename T_scale, typename T_shape>
inline typename return_type<T_y, T_loc, T_scale, T_shape>::type skew_normal_log(
    const T_y& y, const T_loc& mu, const T_scale& sigma, const T_shape& alpha) {
  return skew_normal_lpdf<T_y, T_loc, T_scale, T_shape>(y, mu, sigma, alpha);
}

}  // namespace math
}  // namespace stan
#endif
