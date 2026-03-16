#ifndef STAN_MATH_PRIM_MAT_PROB_LKJ_CORR_CHOLESKY_LOG_HPP
#define STAN_MATH_PRIM_MAT_PROB_LKJ_CORR_CHOLESKY_LOG_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/prob/lkj_corr_cholesky_lpdf.hpp>
#include <boost/math/tools/promotion.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>lkj_corr_cholesky_lpdf</code>
 */
template <bool propto, typename T_covar, typename T_shape>
typename boost::math::tools::promote_args<T_covar, T_shape>::type
lkj_corr_cholesky_log(
    const Eigen::Matrix<T_covar, Eigen::Dynamic, Eigen::Dynamic>& L,
    const T_shape& eta) {
  return lkj_corr_cholesky_lpdf<propto, T_covar, T_shape>(L, eta);
}

/**
 * @deprecated use <code>lkj_corr_cholesky_lpdf</code>
 */
template <typename T_covar, typename T_shape>
inline typename boost::math::tools::promote_args<T_covar, T_shape>::type
lkj_corr_cholesky_log(
    const Eigen::Matrix<T_covar, Eigen::Dynamic, Eigen::Dynamic>& L,
    const T_shape& eta) {
  return lkj_corr_cholesky_lpdf<T_covar, T_shape>(L, eta);
}

}  // namespace math
}  // namespace stan
#endif
