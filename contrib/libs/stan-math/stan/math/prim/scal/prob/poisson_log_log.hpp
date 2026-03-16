#ifndef STAN_MATH_PRIM_SCAL_PROB_POISSON_LOG_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_POISSON_LOG_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/poisson_log_lpmf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>poisson_log_lpmf</code>
 */
template <bool propto, typename T_n, typename T_log_rate>
typename return_type<T_log_rate>::type poisson_log_log(
    const T_n& n, const T_log_rate& alpha) {
  return poisson_log_lpmf<propto, T_n, T_log_rate>(n, alpha);
}

/**
 * @deprecated use <code>poisson_log_lpmf</code>
 */
template <typename T_n, typename T_log_rate>
inline typename return_type<T_log_rate>::type poisson_log_log(
    const T_n& n, const T_log_rate& alpha) {
  return poisson_log_lpmf<T_n, T_log_rate>(n, alpha);
}

}  // namespace math
}  // namespace stan
#endif
