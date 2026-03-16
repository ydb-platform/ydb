#ifndef STAN_MATH_PRIM_MAT_PROB_LKJ_COV_LOG_HPP
#define STAN_MATH_PRIM_MAT_PROB_LKJ_COV_LOG_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/prob/lkj_cov_lpdf.hpp>
#include <boost/math/tools/promotion.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>lkj_cov_lpdf</code>
 */
template <bool propto, typename T_y, typename T_loc, typename T_scale,
          typename T_shape>
typename boost::math::tools::promote_args<T_y, T_loc, T_scale, T_shape>::type
lkj_cov_log(const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
            const Eigen::Matrix<T_loc, Eigen::Dynamic, 1>& mu,
            const Eigen::Matrix<T_scale, Eigen::Dynamic, 1>& sigma,
            const T_shape& eta) {
  return lkj_cov_lpdf<propto, T_y, T_loc, T_scale, T_shape>(y, mu, sigma, eta);
}

/**
 * @deprecated use <code>lkj_cov_lpdf</code>
 */
template <typename T_y, typename T_loc, typename T_scale, typename T_shape>
inline typename boost::math::tools::promote_args<T_y, T_loc, T_scale,
                                                 T_shape>::type
lkj_cov_log(const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
            const Eigen::Matrix<T_loc, Eigen::Dynamic, 1>& mu,
            const Eigen::Matrix<T_scale, Eigen::Dynamic, 1>& sigma,
            const T_shape& eta) {
  return lkj_cov_lpdf<T_y, T_loc, T_scale, T_shape>(y, mu, sigma, eta);
}

/**
 * @deprecated use <code>lkj_cov_lpdf</code>
 */
template <bool propto, typename T_y, typename T_loc, typename T_scale,
          typename T_shape>
typename boost::math::tools::promote_args<T_y, T_loc, T_scale, T_shape>::type
lkj_cov_log(const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
            const T_loc& mu, const T_scale& sigma, const T_shape& eta) {
  return lkj_cov_lpdf<propto, T_y, T_loc, T_scale, T_shape>(y, mu, sigma, eta);
}

/**
 * @deprecated use <code>lkj_cov_lpdf</code>
 */
template <typename T_y, typename T_loc, typename T_scale, typename T_shape>
inline typename boost::math::tools::promote_args<T_y, T_loc, T_scale,
                                                 T_shape>::type
lkj_cov_log(const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y,
            const T_loc& mu, const T_scale& sigma, const T_shape& eta) {
  return lkj_cov_lpdf<T_y, T_loc, T_scale, T_shape>(y, mu, sigma, eta);
}

}  // namespace math
}  // namespace stan
#endif
