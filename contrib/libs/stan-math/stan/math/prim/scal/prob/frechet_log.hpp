#ifndef STAN_MATH_PRIM_SCAL_PROB_FRECHET_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_FRECHET_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/frechet_lpdf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>frechet_lpdf</code>
 */
template <bool propto, typename T_y, typename T_shape, typename T_scale>
typename return_type<T_y, T_shape, T_scale>::type frechet_log(
    const T_y& y, const T_shape& alpha, const T_scale& sigma) {
  return frechet_lpdf<propto, T_y, T_shape, T_scale>(y, alpha, sigma);
}

/**
 * @deprecated use <code>frechet_lpdf</code>
 */
template <typename T_y, typename T_shape, typename T_scale>
inline typename return_type<T_y, T_shape, T_scale>::type frechet_log(
    const T_y& y, const T_shape& alpha, const T_scale& sigma) {
  return frechet_lpdf<T_y, T_shape, T_scale>(y, alpha, sigma);
}

}  // namespace math
}  // namespace stan
#endif
