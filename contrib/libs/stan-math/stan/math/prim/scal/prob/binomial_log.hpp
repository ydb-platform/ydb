#ifndef STAN_MATH_PRIM_SCAL_PROB_BINOMIAL_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BINOMIAL_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/binomial_lpmf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>binomial_lpmf</code>
 */
template <bool propto, typename T_n, typename T_N, typename T_prob>
typename return_type<T_prob>::type binomial_log(const T_n& n, const T_N& N,
                                                const T_prob& theta) {
  return binomial_lpmf<propto, T_n, T_N, T_prob>(n, N, theta);
}

/**
 * @deprecated use <code>binomial_lpmf</code>
 */
template <typename T_n, typename T_N, typename T_prob>
inline typename return_type<T_prob>::type binomial_log(const T_n& n,
                                                       const T_N& N,
                                                       const T_prob& theta) {
  return binomial_lpmf<T_n, T_N, T_prob>(n, N, theta);
}

}  // namespace math
}  // namespace stan
#endif
