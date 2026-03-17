#ifndef STAN_MATH_PRIM_SCAL_PROB_BERNOULLI_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BERNOULLI_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/bernoulli_lpmf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>bernoulli_lpmf</code>
 */
template <bool propto, typename T_n, typename T_prob>
typename return_type<T_prob>::type bernoulli_log(const T_n& n,
                                                 const T_prob& theta) {
  return bernoulli_lpmf<propto, T_n, T_prob>(n, theta);
}

/**
 * @deprecated use <code>bernoulli_lpmf</code>
 */
template <typename T_y, typename T_prob>
inline typename return_type<T_prob>::type bernoulli_log(const T_y& n,
                                                        const T_prob& theta) {
  return bernoulli_lpmf<T_y, T_prob>(n, theta);
}

}  // namespace math
}  // namespace stan
#endif
