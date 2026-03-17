#ifndef STAN_MATH_PRIM_MAT_PROB_MULTINOMIAL_LOG_HPP
#define STAN_MATH_PRIM_MAT_PROB_MULTINOMIAL_LOG_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/prob/multinomial_lpmf.hpp>
#include <boost/math/tools/promotion.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * @deprecated use <code>multinomial_lpmf</code>
 */
template <bool propto, typename T_prob>
typename boost::math::tools::promote_args<T_prob>::type multinomial_log(
    const std::vector<int>& ns,
    const Eigen::Matrix<T_prob, Eigen::Dynamic, 1>& theta) {
  return multinomial_lpmf<propto, T_prob>(ns, theta);
}

/**
 * @deprecated use <code>multinomial_lpmf</code>
 */
template <typename T_prob>
typename boost::math::tools::promote_args<T_prob>::type multinomial_log(
    const std::vector<int>& ns,
    const Eigen::Matrix<T_prob, Eigen::Dynamic, 1>& theta) {
  return multinomial_lpmf<false>(ns, theta);
}

}  // namespace math
}  // namespace stan
#endif
