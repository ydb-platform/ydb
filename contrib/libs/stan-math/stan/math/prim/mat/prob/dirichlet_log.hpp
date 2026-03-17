#ifndef STAN_MATH_PRIM_MAT_PROB_DIRICHLET_LOG_HPP
#define STAN_MATH_PRIM_MAT_PROB_DIRICHLET_LOG_HPP

#include <stan/math/prim/mat/prob/dirichlet_lpmf.hpp>
#include <boost/math/tools/promotion.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>

namespace stan {
namespace math {

/**
 * The log of the Dirichlet density for the given theta and
 * a vector of prior sample sizes, alpha.
 * Each element of alpha must be greater than 0.
 * Each element of theta must be greater than or 0.
 * Theta sums to 1.
 *
 * @deprecated use <code>dirichlet_lpmf</code>
 *
 * @param theta A scalar vector.
 * @param alpha Prior sample sizes.
 * @return The log of the Dirichlet density.
 * @throw std::domain_error if any element of alpha is less than
 * or equal to 0.
 * @throw std::domain_error if any element of theta is less than 0.
 * @throw std::domain_error if the sum of theta is not 1.
 * @tparam T_prob Type of scalar.
 * @tparam T_prior_size Type of prior sample sizes.
 */
template <bool propto, typename T_prob, typename T_prior_size>
typename return_type<T_prob, T_prior_size>::type dirichlet_log(
    const T_prob& theta, const T_prior_size& alpha) {
  return dirichlet_lpmf<propto, T_prob, T_prior_size>(theta, alpha);
}

/**
 * @deprecated use <code>dirichlet_lpmf</code>
 */
template <typename T_prob, typename T_prior_size>
typename return_type<T_prob, T_prior_size>::type dirichlet_log(
    const T_prob& theta, const T_prior_size& alpha) {
  return dirichlet_lpmf<T_prob, T_prior_size>(theta, alpha);
}

}  // namespace math
}  // namespace stan
#endif
