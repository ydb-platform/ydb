#ifndef STAN_MATH_PRIM_SCAL_PROB_EXPONENTIAL_CCDF_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_EXPONENTIAL_CCDF_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/exponential_lccdf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>exponential_lccdf</code>
 */
template <typename T_y, typename T_inv_scale>
typename return_type<T_y, T_inv_scale>::type exponential_ccdf_log(
    const T_y& y, const T_inv_scale& beta) {
  return exponential_lccdf<T_y, T_inv_scale>(y, beta);
}

}  // namespace math
}  // namespace stan
#endif
