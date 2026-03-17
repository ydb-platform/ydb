#ifndef STAN_MATH_PRIM_SCAL_PROB_LOGISTIC_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_LOGISTIC_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/logistic_lpdf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>logistic_lpdf</code>
 */
template <bool propto, typename T_y, typename T_loc, typename T_scale>
typename return_type<T_y, T_loc, T_scale>::type logistic_log(
    const T_y& y, const T_loc& mu, const T_scale& sigma) {
  return logistic_lpdf<propto, T_y, T_loc, T_scale>(y, mu, sigma);
}

/**
 * @deprecated use <code>logistic_lpdf</code>
 */
template <typename T_y, typename T_loc, typename T_scale>
inline typename return_type<T_y, T_loc, T_scale>::type logistic_log(
    const T_y& y, const T_loc& mu, const T_scale& sigma) {
  return logistic_lpdf<T_y, T_loc, T_scale>(y, mu, sigma);
}

}  // namespace math
}  // namespace stan
#endif
