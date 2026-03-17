#ifndef STAN_MATH_PRIM_SCAL_PROB_NEG_BINOMIAL_2_LOG_HPP
#define STAN_MATH_PRIM_SCAL_PROB_NEG_BINOMIAL_2_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/prob/neg_binomial_2_lpmf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>neg_binomial_2_lpmf</code>
 */
template <bool propto, typename T_n, typename T_location, typename T_precision>
typename return_type<T_location, T_precision>::type neg_binomial_2_log(
    const T_n& n, const T_location& mu, const T_precision& phi) {
  return neg_binomial_2_lpmf<propto, T_n, T_location, T_precision>(n, mu, phi);
}

/**
 * @deprecated use <code>neg_binomial_2_lpmf</code>
 */
template <typename T_n, typename T_location, typename T_precision>
inline typename return_type<T_location, T_precision>::type neg_binomial_2_log(
    const T_n& n, const T_location& mu, const T_precision& phi) {
  return neg_binomial_2_lpmf<T_n, T_location, T_precision>(n, mu, phi);
}

}  // namespace math
}  // namespace stan
#endif
