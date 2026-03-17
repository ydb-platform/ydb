#ifndef STAN_MATH_PRIM_MAT_PROB_CATEGORICAL_LOG_HPP
#define STAN_MATH_PRIM_MAT_PROB_CATEGORICAL_LOG_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/prob/categorical_lpmf.hpp>
#include <boost/math/tools/promotion.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * @deprecated use <code>categorical_lpmf</code>
 */
template <bool propto, typename T_prob>
typename boost::math::tools::promote_args<T_prob>::type categorical_log(
    int n, const Eigen::Matrix<T_prob, Eigen::Dynamic, 1>& theta) {
  return categorical_lpmf<propto, T_prob>(n, theta);
}

/**
 * @deprecated use <code>categorical_lpmf</code>
 */
template <typename T_prob>
typename boost::math::tools::promote_args<T_prob>::type categorical_log(
    const typename math::index_type<Eigen::VectorXd>::type n,
    const Eigen::Matrix<T_prob, Eigen::Dynamic, 1>& theta) {
  return categorical_lpmf<T_prob>(n, theta);
}

/**
 * @deprecated use <code>categorical_lpmf</code>
 */
template <bool propto, typename T_prob>
typename boost::math::tools::promote_args<T_prob>::type categorical_log(
    const std::vector<int>& ns,
    const Eigen::Matrix<T_prob, Eigen::Dynamic, 1>& theta) {
  return categorical_lpmf<propto, T_prob>(ns, theta);
}

/**
 * @deprecated use <code>categorical_lpmf</code>
 */
template <typename T_prob>
inline typename boost::math::tools::promote_args<T_prob>::type categorical_log(
    const std::vector<int>& ns,
    const Eigen::Matrix<T_prob, Eigen::Dynamic, 1>& theta) {
  return categorical_lpmf<false>(ns, theta);
}

}  // namespace math
}  // namespace stan
#endif
