#ifndef STAN_MATH_PRIM_SCAL_PROB_RAYLEIGH_CDF_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_RAYLEIGH_CDF_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/rayleigh_lcdf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>rayleigh_lcdf</code>
 */
template <typename T_y, typename T_scale>
typename return_type<T_y, T_scale>::type rayleigh_cdf_log(
    const T_y& y, const T_scale& sigma) {
  return rayleigh_lcdf<T_y, T_scale>(y, sigma);
}

}  // namespace math
}  // namespace stan
#endif
