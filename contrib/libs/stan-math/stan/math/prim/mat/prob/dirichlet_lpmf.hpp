#ifndef STAN_MATH_PRIM_MAT_PROB_DIRICHLET_LPMF_HPP
#define STAN_MATH_PRIM_MAT_PROB_DIRICHLET_LPMF_HPP

#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/return_type.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/mat/err/check_simplex.hpp>
#include <stan/math/prim/mat/fun/lgamma.hpp>
#include <stan/math/prim/mat/fun/digamma.hpp>
#include <stan/math/prim/mat/fun/value_of.hpp>
#include <stan/math/prim/mat/meta/is_vector.hpp>
#include <stan/math/prim/mat/meta/operands_and_partials.hpp>
#include <stan/math/prim/mat/meta/is_constant_struct.hpp>
#include <stan/math/prim/mat/meta/get.hpp>
#include <stan/math/prim/mat/meta/length.hpp>
#include <stan/math/prim/mat/meta/vector_seq_view.hpp>

namespace stan {
namespace math {

/**
 * The log of the Dirichlet density for the given theta and
 * a vector of prior sample sizes, alpha.
 * Each element of alpha must be greater than 0.
 * Each element of theta must be greater than or 0.
 * Theta sums to 1.
 *
 * \f[
 * \theta\sim\mbox{Dirichlet}(\alpha_1,\ldots,\alpha_k)\\
 * \log(p(\theta\,|\,\alpha_1,\ldots,\alpha_k))=\log\left(\frac{\Gamma(\alpha_1+\cdots+\alpha_k)}{\Gamma(\alpha_1)+\cdots+\Gamma(\alpha_k)}*
 * \left(\theta_1^{\alpha_1-1}+\cdots+\theta_k^{\alpha_k-1}\right)\right)\\
 * =\log(\Gamma(\alpha_1+\cdots+\alpha_k))-\left(\log(\Gamma(\alpha_1))+\cdots+\log(\Gamma(\alpha_k))\right)+(\alpha_1-1)\log(\theta_1)+\cdots+(\alpha_k-1)\log(\theta_k)
 * \f]
 *
 * \f[
 * \frac{\partial }{\partial
 * \theta_x}\log(p(\theta\,|\,\alpha_1,\ldots,\alpha_k))=\frac{\alpha_x-1}{\theta_x}
 * \f]
 *
 * \f[
 * \frac{\partial}{\partial\alpha_x}\log(p(\theta\,|\,\alpha_1,\ldots,\alpha_k))=\psi_{(0)}(\sum\alpha)-\psi_{(0)}(\alpha_x)+\log\theta_x
 * \f]
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
typename return_type<T_prob, T_prior_size>::type dirichlet_lpmf(
    const T_prob& theta, const T_prior_size& alpha) {
  static const char* function = "dirichlet_lpmf";

  typedef typename stan::partials_return_type<T_prob, T_prior_size>::type
      T_partials_return;
  typedef typename Eigen::Matrix<T_partials_return, -1, 1> T_partials_vec;

  check_consistent_sizes(function, "probabilities", theta, "prior sample sizes",
                         alpha);
  check_positive(function, "prior sample sizes", alpha);
  check_simplex(function, "probabilities", theta);

  vector_seq_view<T_prob> theta_vec(theta);
  T_partials_vec theta_dbl = value_of(theta_vec[0]);

  vector_seq_view<T_prior_size> alpha_vec(alpha);
  T_partials_vec alpha_dbl = value_of(alpha_vec[0]);

  T_partials_return lp(0.0);

  if (include_summand<propto, T_prior_size>::value)
    lp += lgamma(alpha_dbl.sum()) - lgamma(alpha_dbl).sum();

  if (include_summand<propto, T_prob, T_prior_size>::value)
    lp += (theta_dbl.array().log() * (alpha_dbl.array() - 1.0)).sum();

  T_partials_vec theta_deriv = (alpha_dbl.array() - 1.0) / theta_dbl.array();

  T_partials_vec alpha_deriv = digamma(alpha_dbl.sum())
                               - digamma(alpha_dbl).array()
                               + theta_dbl.array().log();

  operands_and_partials<T_prob, T_prior_size> ops_partials(theta, alpha);
  if (!is_constant_struct<T_prob>::value)
    ops_partials.edge1_.partials_ = theta_deriv;

  if (!is_constant_struct<T_prior_size>::value)
    ops_partials.edge2_.partials_ = alpha_deriv;

  return ops_partials.build(lp);
}

template <typename T_prob, typename T_prior_size>
typename return_type<T_prob, T_prior_size>::type dirichlet_lpmf(
    const T_prob& theta, const T_prior_size& alpha) {
  return dirichlet_lpmf<false>(theta, alpha);
}

}  // namespace math
}  // namespace stan
#endif
