#ifndef STAN_MATH_PRIM_MAT_PROB_MULTI_GP_CHOLESKY_LOG_HPP
#define STAN_MATH_PRIM_MAT_PROB_MULTI_GP_CHOLESKY_LOG_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/prob/multi_gp_cholesky_lpdf.hpp>
#include <boost/math/tools/promotion.hpp>

namespace stan {
namespace math {
/**
 * The log of a multivariate Gaussian Process for the given y, w, and
 * a Cholesky factor L of the kernel matrix Sigma.
 * Sigma = LL', a square, semi-positive definite matrix.
 * y is a dxN matrix, where each column is a different observation and each
 * row is a different output dimension.  The Gaussian Process is assumed to
 * have a scaled kernel matrix with a different scale for each output dimension.
 * This distribution is equivalent to:
 *    for (i in 1:d) row(y, i) ~ multi_normal(0, (1/w[i])*LL').
 *
 * @deprecated use <code>multi_gp_cholesky_lpdf</code>
 *
 * @param y A dxN matrix
 * @param L The Cholesky decomposition of a kernel matrix
 * @param w A d-dimensional vector of positve inverse scale parameters for each
 * output.
 * @return The log of the multivariate GP density.
 * @throw std::domain_error if Sigma is not square, not symmetric,
 * or not semi-positive definite.
 * @tparam T_y Type of scalar.
 * @tparam T_covar Type of kernel.
 * @tparam T_w Type of weight.
 */
template <bool propto, typename T_y, typename T_covar, typename T_w>
typename boost::math::tools::promote_args<T_y, T_covar, T_w>::type
multi_gp_cholesky_log(
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
    const Eigen::Matrix<T_covar, Eigen::Dynamic, Eigen::Dynamic>& L,
    const Eigen::Matrix<T_w, Eigen::Dynamic, 1>& w) {
  return multi_gp_cholesky_lpdf<propto, T_y, T_covar, T_w>(y, L, w);
}

/**
 * @deprecated use <code>multi_gp_cholesky_lpdf</code>
 */
template <typename T_y, typename T_covar, typename T_w>
inline typename boost::math::tools::promote_args<T_y, T_covar, T_w>::type
multi_gp_cholesky_log(
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
    const Eigen::Matrix<T_covar, Eigen::Dynamic, Eigen::Dynamic>& L,
    const Eigen::Matrix<T_w, Eigen::Dynamic, 1>& w) {
  return multi_gp_cholesky_lpdf<T_y, T_covar, T_w>(y, L, w);
}

}  // namespace math
}  // namespace stan
#endif
