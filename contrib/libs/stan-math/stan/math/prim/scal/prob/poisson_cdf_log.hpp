#ifndef STAN_MATH_PRIM_SCAL_PROB_POISSON_CDF_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_POISSON_CDF_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/poisson_lcdf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>poisson_lcdf</code>
 */
template <typename T_n, typename T_rate>
typename return_type<T_rate>::type poisson_cdf_log(const T_n& n,
                                                   const T_rate& lambda) {
  return poisson_lcdf<T_n, T_rate>(n, lambda);
}

}  // namespace math
}  // namespace stan
#endif
