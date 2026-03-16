#ifndef STAN_MATH_PRIM_SCAL_PROB_SCALED_INV_CHI_SQUARE_CDF_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_SCALED_INV_CHI_SQUARE_CDF_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/scaled_inv_chi_square_lcdf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>scaled_inv_chi_square_lcdf</code>
 */
template <typename T_y, typename T_dof, typename T_scale>
typename return_type<T_y, T_dof, T_scale>::type scaled_inv_chi_square_cdf_log(
    const T_y& y, const T_dof& nu, const T_scale& s) {
  return scaled_inv_chi_square_lcdf<T_y, T_dof, T_scale>(y, nu, s);
}

}  // namespace math
}  // namespace stan
#endif
