#ifndef STAN_MATH_PRIM_SCAL_PROB_UNIFORM_CCDF_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_UNIFORM_CCDF_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/uniform_lccdf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>uniform_lccdf</code>
 */
template <typename T_y, typename T_low, typename T_high>
typename return_type<T_y, T_low, T_high>::type uniform_ccdf_log(
    const T_y& y, const T_low& alpha, const T_high& beta) {
  return uniform_lccdf<T_y, T_low, T_high>(y, alpha, beta);
}

}  // namespace math
}  // namespace stan
#endif
