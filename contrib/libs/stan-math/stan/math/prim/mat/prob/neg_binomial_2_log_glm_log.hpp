#ifndef STAN_MATH_PRIM_MAT_PROB_NEG_BINOMIAL_2_LOG_GLM_LOG_HPP
#define STAN_MATH_PRIM_MAT_PROB_NEG_BINOMIAL_2_LOG_GLM_LOG_HPP

#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/mat/prob/neg_binomial_2_log_glm_lpmf.hpp>

namespace stan {
namespace math {

/**
 * @deprecated use <code>neg_binomial_2_log_glm_lpmf</code>
 */
template <bool propto, typename T_y, typename T_x, typename T_alpha,
          typename T_beta, typename T_precision>
typename return_type<T_x, T_alpha, T_beta, T_precision>::type
neg_binomial_2_log_glm_log(const T_y &y, const T_x &x, const T_alpha &alpha,
                           const T_beta &beta, const T_precision &phi) {
  return neg_binomial_2_log_glm_lpmf<propto, T_y, T_x, T_alpha, T_beta,
                                     T_precision>(y, x, alpha, beta, phi);
}

/**
 * @deprecated use <code>poisson_logit_glm_lpmf</code>
 */
template <typename T_y, typename T_x, typename T_alpha, typename T_beta,
          typename T_precision>
inline typename return_type<T_x, T_alpha, T_beta, T_precision>::type
neg_binomial_2_log_glm_log(const T_y &y, const T_x &x, const T_alpha &alpha,
                           const T_beta &beta, const T_precision &phi) {
  return neg_binomial_2_log_glm_lpmf<false>(y, x, alpha, beta, phi);
}
}  // namespace math
}  // namespace stan
#endif
