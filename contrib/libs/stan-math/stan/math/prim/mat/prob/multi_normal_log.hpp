#ifndef STAN_MATH_PRIM_MAT_PROB_MULTI_NORMAL_LOG_HPP
#define STAN_MATH_PRIM_MAT_PROB_MULTI_NORMAL_LOG_HPP

#include <stan/math/prim/mat/prob/multi_normal_lpdf.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>matrix_normal_lpdf</code>
 */
template <bool propto, typename T_y, typename T_loc, typename T_covar>
typename return_type<T_y, T_loc, T_covar>::type multi_normal_log(
    const T_y& y, const T_loc& mu, const T_covar& Sigma) {
  return multi_normal_lpdf<propto, T_y, T_loc, T_covar>(y, mu, Sigma);
}

/**
 * @deprecated use <code>matrix_normal_lpdf</code>
 */
template <typename T_y, typename T_loc, typename T_covar>
inline typename return_type<T_y, T_loc, T_covar>::type multi_normal_log(
    const T_y& y, const T_loc& mu, const T_covar& Sigma) {
  return multi_normal_lpdf<T_y, T_loc, T_covar>(y, mu, Sigma);
}

}  // namespace math
}  // namespace stan
#endif
