#ifndef STAN_MATH_PRIM_SCAL_PROB_BETA_CCDF_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BETA_CCDF_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/beta_lccdf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>beta_lccdf</code>
 */
template <typename T_y, typename T_scale_succ, typename T_scale_fail>
typename return_type<T_y, T_scale_succ, T_scale_fail>::type beta_ccdf_log(
    const T_y& y, const T_scale_succ& alpha, const T_scale_fail& beta) {
  return beta_lccdf<T_y, T_scale_succ, T_scale_fail>(y, alpha, beta);
}

}  // namespace math
}  // namespace stan
#endif
