#ifndef STAN_MATH_PRIM_MAT_PROB_GAUSSIAN_DLM_OBS_LOG_HPP
#define STAN_MATH_PRIM_MAT_PROB_GAUSSIAN_DLM_OBS_LOG_HPP

#include <stan/math/prim/mat/prob/gaussian_dlm_obs_lpdf.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>

namespace stan {
namespace math {
/**
 * The log of a Gaussian dynamic linear model (GDLM).
 * This distribution is equivalent to, for \f$t = 1:T\f$,
 * \f{eqnarray*}{
 * y_t & \sim N(F' \theta_t, V) \\
 * \theta_t & \sim N(G \theta_{t-1}, W) \\
 * \theta_0 & \sim N(m_0, C_0)
 * \f}
 *
 * If V is a vector, then the Kalman filter is applied
 * sequentially.
 *
 * @deprecated use <code>gaussian_dlm_obs_lpdf</code>
 *
 * @param y A r x T matrix of observations. Rows are variables,
 * columns are observations.
 * @param F A n x r matrix. The design matrix.
 * @param G A n x n matrix. The transition matrix.
 * @param V A r x r matrix. The observation covariance matrix.
 * @param W A n x n matrix. The state covariance matrix.
 * @param m0 A n x 1 matrix. The mean vector of the distribution
 * of the initial state.
 * @param C0 A n x n matrix. The covariance matrix of the
 * distribution of the initial state.
 * @return The log of the joint density of the GDLM.
 * @throw std::domain_error if a matrix in the Kalman filter is
 * not positive semi-definite.
 * @tparam T_y Type of scalar.
 * @tparam T_F Type of design matrix.
 * @tparam T_G Type of transition matrix.
 * @tparam T_V Type of observation covariance matrix.
 * @tparam T_W Type of state covariance matrix.
 * @tparam T_m0 Type of initial state mean vector.
 * @tparam T_C0 Type of initial state covariance matrix.
 */
template <bool propto, typename T_y, typename T_F, typename T_G, typename T_V,
          typename T_W, typename T_m0, typename T_C0>
typename return_type<
    T_y, typename return_type<T_F, T_G, T_V, T_W, T_m0, T_C0>::type>::type
gaussian_dlm_obs_log(
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
    const Eigen::Matrix<T_F, Eigen::Dynamic, Eigen::Dynamic>& F,
    const Eigen::Matrix<T_G, Eigen::Dynamic, Eigen::Dynamic>& G,
    const Eigen::Matrix<T_V, Eigen::Dynamic, Eigen::Dynamic>& V,
    const Eigen::Matrix<T_W, Eigen::Dynamic, Eigen::Dynamic>& W,
    const Eigen::Matrix<T_m0, Eigen::Dynamic, 1>& m0,
    const Eigen::Matrix<T_C0, Eigen::Dynamic, Eigen::Dynamic>& C0) {
  return gaussian_dlm_obs_lpdf<propto, T_y, T_F, T_G, T_V, T_W, T_m0, T_C0>(
      y, F, G, V, W, m0, C0);
}

/**
 * @deprecated use <code>gaussian_dlm_obs_lpdf</code>
 */
template <typename T_y, typename T_F, typename T_G, typename T_V, typename T_W,
          typename T_m0, typename T_C0>
inline typename return_type<
    T_y, typename return_type<T_F, T_G, T_V, T_W, T_m0, T_C0>::type>::type
gaussian_dlm_obs_log(
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
    const Eigen::Matrix<T_F, Eigen::Dynamic, Eigen::Dynamic>& F,
    const Eigen::Matrix<T_G, Eigen::Dynamic, Eigen::Dynamic>& G,
    const Eigen::Matrix<T_V, Eigen::Dynamic, Eigen::Dynamic>& V,
    const Eigen::Matrix<T_W, Eigen::Dynamic, Eigen::Dynamic>& W,
    const Eigen::Matrix<T_m0, Eigen::Dynamic, 1>& m0,
    const Eigen::Matrix<T_C0, Eigen::Dynamic, Eigen::Dynamic>& C0) {
  return gaussian_dlm_obs_lpdf<T_y, T_F, T_G, T_V, T_W, T_m0, T_C0>(y, F, G, V,
                                                                    W, m0, C0);
}

/**
 * The log of a Gaussian dynamic linear model (GDLM) with
 * uncorrelated observation disturbances.
 * This distribution is equivalent to, for \f$t = 1:T\f$,
 * \f{eqnarray*}{
 * y_t & \sim N(F' \theta_t, diag(V)) \\
 * \theta_t & \sim N(G \theta_{t-1}, W) \\
 * \theta_0 & \sim N(m_0, C_0)
 * \f}
 *
 * If V is a vector, then the Kalman filter is applied
 * sequentially.
 *
 * @deprecated use <code>gaussian_dlm_obs_lpdf</code>
 *
 * @param y A r x T matrix of observations. Rows are variables,
 * columns are observations.
 * @param F A n x r matrix. The design matrix.
 * @param G A n x n matrix. The transition matrix.
 * @param V A size r vector. The diagonal of the observation
 * covariance matrix.
 * @param W A n x n matrix. The state covariance matrix.
 * @param m0 A n x 1 matrix. The mean vector of the distribution
 * of the initial state.
 * @param C0 A n x n matrix. The covariance matrix of the
 * distribution of the initial state.
 * @return The log of the joint density of the GDLM.
 * @throw std::domain_error if a matrix in the Kalman filter is
 * not semi-positive definite.
 * @tparam T_y Type of scalar.
 * @tparam T_F Type of design matrix.
 * @tparam T_G Type of transition matrix.
 * @tparam T_V Type of observation variances
 * @tparam T_W Type of state covariance matrix.
 * @tparam T_m0 Type of initial state mean vector.
 * @tparam T_C0 Type of initial state covariance matrix.
 */
template <bool propto, typename T_y, typename T_F, typename T_G, typename T_V,
          typename T_W, typename T_m0, typename T_C0>
typename return_type<
    T_y, typename return_type<T_F, T_G, T_V, T_W, T_m0, T_C0>::type>::type
gaussian_dlm_obs_log(
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
    const Eigen::Matrix<T_F, Eigen::Dynamic, Eigen::Dynamic>& F,
    const Eigen::Matrix<T_G, Eigen::Dynamic, Eigen::Dynamic>& G,
    const Eigen::Matrix<T_V, Eigen::Dynamic, 1>& V,
    const Eigen::Matrix<T_W, Eigen::Dynamic, Eigen::Dynamic>& W,
    const Eigen::Matrix<T_m0, Eigen::Dynamic, 1>& m0,
    const Eigen::Matrix<T_C0, Eigen::Dynamic, Eigen::Dynamic>& C0) {
  return gaussian_dlm_obs_lpdf<propto, T_y, T_F, T_G, T_V, T_W, T_m0, T_C0>(
      y, F, G, V, W, m0, C0);
}

/**
 * @deprecated use <code>gaussian_dlm_obs_lpdf</code>
 */
template <typename T_y, typename T_F, typename T_G, typename T_V, typename T_W,
          typename T_m0, typename T_C0>
inline typename return_type<
    T_y, typename return_type<T_F, T_G, T_V, T_W, T_m0, T_C0>::type>::type
gaussian_dlm_obs_log(
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
    const Eigen::Matrix<T_F, Eigen::Dynamic, Eigen::Dynamic>& F,
    const Eigen::Matrix<T_G, Eigen::Dynamic, Eigen::Dynamic>& G,
    const Eigen::Matrix<T_V, Eigen::Dynamic, 1>& V,
    const Eigen::Matrix<T_W, Eigen::Dynamic, Eigen::Dynamic>& W,
    const Eigen::Matrix<T_m0, Eigen::Dynamic, 1>& m0,
    const Eigen::Matrix<T_C0, Eigen::Dynamic, Eigen::Dynamic>& C0) {
  return gaussian_dlm_obs_lpdf<T_y, T_F, T_G, T_V, T_W, T_m0, T_C0>(y, F, G, V,
                                                                    W, m0, C0);
}

}  // namespace math
}  // namespace stan
#endif
