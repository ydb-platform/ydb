#ifndef STAN_MATH_PRIM_MAT_PROB_WISHART_LOG_HPP
#define STAN_MATH_PRIM_MAT_PROB_WISHART_LOG_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/prob/wishart_lpdf.hpp>
#include <boost/math/tools/promotion.hpp>

namespace stan {
namespace math {

/**
 * The log of the Wishart density for the given W, degrees of freedom,
 * and scale matrix.
 *
 * The scale matrix, S, must be k x k, symmetric, and semi-positive definite.
 * Dimension, k, is implicit.
 * nu must be greater than k-1
 *
 * \f{eqnarray*}{
 W &\sim& \mbox{\sf{Wishart}}_{\nu} (S) \\
 \log (p (W \, |\, \nu, S) ) &=& \log \left( \left(2^{\nu k/2} \pi^{k (k-1) /4}
 \prod_{i=1}^k{\Gamma (\frac{\nu + 1 - i}{2})} \right)^{-1} \times \left| S
 \right|^{-\nu/2} \left| W \right|^{(\nu - k - 1) / 2}
 \times \exp (-\frac{1}{2} \mbox{tr} (S^{-1} W)) \right) \\
 &=& -\frac{\nu k}{2}\log(2) - \frac{k (k-1)}{4} \log(\pi) - \sum_{i=1}^{k}{\log
 (\Gamma (\frac{\nu+1-i}{2}))}
 -\frac{\nu}{2} \log(\det(S)) + \frac{\nu-k-1}{2}\log (\det(W)) - \frac{1}{2}
 \mbox{tr} (S^{-1}W) \f}
 *
 * @deprecated use <code>wishart_lpdf</code>
 *
 * @param W A scalar matrix
 * @param nu Degrees of freedom
 * @param S The scale matrix
 * @return The log of the Wishart density at W given nu and S.
 * @throw std::domain_error if nu is not greater than k-1
 * @throw std::domain_error if S is not square, not symmetric, or not
 semi-positive definite.
 * @tparam T_y Type of scalar.
 * @tparam T_dof Type of degrees of freedom.
 * @tparam T_scale Type of scale.
 */
template <bool propto, typename T_y, typename T_dof, typename T_scale>
typename boost::math::tools::promote_args<T_y, T_dof, T_scale>::type
wishart_log(const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& W,
            const T_dof& nu,
            const Eigen::Matrix<T_scale, Eigen::Dynamic, Eigen::Dynamic>& S) {
  return wishart_lpdf<propto, T_y, T_dof, T_scale>(W, nu, S);
}

/**
 * @deprecated use <code>wishart_lpdf</code>
 */
template <typename T_y, typename T_dof, typename T_scale>
inline typename boost::math::tools::promote_args<T_y, T_dof, T_scale>::type
wishart_log(const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& W,
            const T_dof& nu,
            const Eigen::Matrix<T_scale, Eigen::Dynamic, Eigen::Dynamic>& S) {
  return wishart_lpdf<T_y, T_dof, T_scale>(W, nu, S);
}

}  // namespace math
}  // namespace stan
#endif
