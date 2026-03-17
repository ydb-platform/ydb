#ifndef STAN_MATH_PRIM_MAT_PROB_INV_WISHART_LOG_HPP
#define STAN_MATH_PRIM_MAT_PROB_INV_WISHART_LOG_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/prob/inv_wishart_lpdf.hpp>
#include <boost/math/tools/promotion.hpp>

namespace stan {
namespace math {

/**
 * The log of the Inverse-Wishart density for the given W, degrees
 * of freedom, and scale matrix.
 *
 * The scale matrix, S, must be k x k, symmetric, and semi-positive
 * definite.
 *
 * @deprecated use <code>inverse_wishart_lpdf</code>
 *
 * @param W A scalar matrix
 * @param nu Degrees of freedom
 * @param S The scale matrix
 * @return The log of the Inverse-Wishart density at W given nu and S.
 * @throw std::domain_error if nu is not greater than k-1
 * @throw std::domain_error if S is not square, not symmetric, or not
 * semi-positive definite.
 * @tparam T_y Type of scalar.
 * @tparam T_dof Type of degrees of freedom.
 * @tparam T_scale Type of scale.
 */
template <bool propto, typename T_y, typename T_dof, typename T_scale>
typename boost::math::tools::promote_args<T_y, T_dof, T_scale>::type
inv_wishart_log(
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& W,
    const T_dof& nu,
    const Eigen::Matrix<T_scale, Eigen::Dynamic, Eigen::Dynamic>& S) {
  return inv_wishart_lpdf<propto, T_y, T_dof, T_scale>(W, nu, S);
}

/**
 * @deprecated use <code>inverse_wishart_lpdf</code>
 */
template <typename T_y, typename T_dof, typename T_scale>
inline typename boost::math::tools::promote_args<T_y, T_dof, T_scale>::type
inv_wishart_log(
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& W,
    const T_dof& nu,
    const Eigen::Matrix<T_scale, Eigen::Dynamic, Eigen::Dynamic>& S) {
  return inv_wishart_lpdf<T_y, T_dof, T_scale>(W, nu, S);
}

}  // namespace math
}  // namespace stan
#endif
