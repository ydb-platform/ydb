#ifndef STAN_MATH_PRIM_SCAL_PROB_CAUCHY_CCDF_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_CAUCHY_CCDF_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/cauchy_lccdf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>cauchy_lccdf</code>
 */
template <typename T_y, typename T_loc, typename T_scale>
typename return_type<T_y, T_loc, T_scale>::type cauchy_ccdf_log(
    const T_y& y, const T_loc& mu, const T_scale& sigma) {
  return cauchy_lccdf<T_y, T_loc, T_scale>(y, mu, sigma);
}

}  // namespace math
}  // namespace stan
#endif
