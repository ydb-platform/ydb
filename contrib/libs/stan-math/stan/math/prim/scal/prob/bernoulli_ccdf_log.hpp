#ifndef STAN_MATH_PRIM_SCAL_PROB_BERNOULLI_CCDF_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BERNOULLI_CCDF_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/bernoulli_lccdf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>bernoulli_lccdf</code>
 */
template <typename T_n, typename T_prob>
typename return_type<T_prob>::type bernoulli_ccdf_log(const T_n& n,
                                                      const T_prob& theta) {
  return bernoulli_lccdf<T_n, T_prob>(n, theta);
}
}  // namespace math
}  // namespace stan
#endif
