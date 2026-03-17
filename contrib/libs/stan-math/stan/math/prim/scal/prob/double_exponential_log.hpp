#ifndef STAN_MATH_PRIM_SCAL_PROB_DOUBLE_EXPONENTIAL_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_DOUBLE_EXPONENTIAL_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/double_exponential_lpdf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>double_exponential_lpdf</code>
 */
template <bool propto, typename T_y, typename T_loc, typename T_scale>
typename return_type<T_y, T_loc, T_scale>::type double_exponential_log(
    const T_y& y, const T_loc& mu, const T_scale& sigma) {
  return double_exponential_lpdf<propto, T_y, T_loc, T_scale>(y, mu, sigma);
}

/**
 * @deprecated use <code>double_exponential_lpdf</code>
 */
template <typename T_y, typename T_loc, typename T_scale>
typename return_type<T_y, T_loc, T_scale>::type double_exponential_log(
    const T_y& y, const T_loc& mu, const T_scale& sigma) {
  return double_exponential_lpdf<T_y, T_loc, T_scale>(y, mu, sigma);
}

}  // namespace math
}  // namespace stan
#endif
