#ifndef STAN_MATH_PRIM_SCAL_PROB_HYPERGEOMETRIC_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_HYPERGEOMETRIC_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/hypergeometric_lpmf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>hypergeometric_lpmf</code>
 */
template <bool propto, typename T_n, typename T_N, typename T_a, typename T_b>
double hypergeometric_log(const T_n& n, const T_N& N, const T_a& a,
                          const T_b& b) {
  return hypergeometric_lpmf<propto, T_n, T_N, T_a, T_b>(n, N, a, b);
}

/**
 * @deprecated use <code>hypergeometric_lpmf</code>
 */
template <typename T_n, typename T_N, typename T_a, typename T_b>
inline double hypergeometric_log(const T_n& n, const T_N& N, const T_a& a,
                                 const T_b& b) {
  return hypergeometric_lpmf<T_n, T_N, T_a, T_b>(n, N, a, b);
}

}  // namespace math
}  // namespace stan
#endif
