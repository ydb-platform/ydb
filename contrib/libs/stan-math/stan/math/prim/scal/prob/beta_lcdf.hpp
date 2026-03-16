#ifndef STAN_MATH_PRIM_SCAL_PROB_BETA_LCDF_HPP
#define STAN_MATH_PRIM_SCAL_PROB_BETA_LCDF_HPP

#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <stan/math/prim/scal/meta/partials_return_type.hpp>
#include <stan/math/prim/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/err/check_consistent_sizes.hpp>
#include <stan/math/prim/scal/err/check_less_or_equal.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive_finite.hpp>
#include <stan/math/prim/scal/fun/size_zero.hpp>
#include <stan/math/prim/scal/fun/log1m.hpp>
#include <stan/math/prim/scal/fun/multiply_log.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>
#include <stan/math/prim/scal/fun/lgamma.hpp>
#include <stan/math/prim/scal/fun/lbeta.hpp>
#include <stan/math/prim/scal/meta/contains_nonconstant_struct.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/scalar_seq_view.hpp>
#include <stan/math/prim/scal/meta/VectorBuilder.hpp>
#include <stan/math/prim/scal/fun/grad_reg_inc_beta.hpp>
#include <stan/math/prim/scal/fun/inc_beta.hpp>
#include <boost/math/special_functions/gamma.hpp>
#include <boost/random/gamma_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>

namespace stan {
namespace math {

/**
 * Returns the beta log cumulative distribution function for the given
 * probability, success, and failure parameters.  Any arguments other
 * than scalars must be containers of the same size.  With non-scalar
 * arguments, the return is the sum of the log cdfs with scalars
 * broadcast as necessary.
 *
 * @tparam T_y type of y
 * @tparam T_scale_succ type of success parameter
 * @tparam T_scale_fail type of failure parameter
 * @param y (Sequence of) scalar(s) between zero and one
 * @param alpha (Sequence of) success parameter(s)
 * @param beta (Sequence of) failure parameter(s)
 * @return log probability or sum of log of proabilities
 * @throw std::domain_error if alpha or beta is negative
 * @throw std::domain_error if y is not a valid probability
 * @throw std::invalid_argument if container sizes mismatch
 */
template <typename T_y, typename T_scale_succ, typename T_scale_fail>
typename return_type<T_y, T_scale_succ, T_scale_fail>::type beta_lcdf(
    const T_y& y, const T_scale_succ& alpha, const T_scale_fail& beta) {
  typedef
      typename stan::partials_return_type<T_y, T_scale_succ, T_scale_fail>::type
          T_partials_return;

  if (size_zero(y, alpha, beta))
    return 0.0;

  static const char* function = "beta_lcdf";

  using boost::math::tools::promote_args;

  T_partials_return cdf_log(0.0);

  check_positive_finite(function, "First shape parameter", alpha);
  check_positive_finite(function, "Second shape parameter", beta);
  check_not_nan(function, "Random variable", y);
  check_nonnegative(function, "Random variable", y);
  check_less_or_equal(function, "Random variable", y, 1);
  check_consistent_sizes(function, "Random variable", y,
                         "First shape parameter", alpha,
                         "Second shape parameter", beta);

  scalar_seq_view<T_y> y_vec(y);
  scalar_seq_view<T_scale_succ> alpha_vec(alpha);
  scalar_seq_view<T_scale_fail> beta_vec(beta);
  size_t N = max_size(y, alpha, beta);

  operands_and_partials<T_y, T_scale_succ, T_scale_fail> ops_partials(y, alpha,
                                                                      beta);

  using std::exp;
  using std::log;
  using std::pow;

  VectorBuilder<contains_nonconstant_struct<T_scale_succ, T_scale_fail>::value,
                T_partials_return, T_scale_succ, T_scale_fail>
      digamma_alpha_vec(max_size(alpha, beta));

  VectorBuilder<contains_nonconstant_struct<T_scale_succ, T_scale_fail>::value,
                T_partials_return, T_scale_succ, T_scale_fail>
      digamma_beta_vec(max_size(alpha, beta));

  VectorBuilder<contains_nonconstant_struct<T_scale_succ, T_scale_fail>::value,
                T_partials_return, T_scale_succ, T_scale_fail>
      digamma_sum_vec(max_size(alpha, beta));

  if (contains_nonconstant_struct<T_scale_succ, T_scale_fail>::value) {
    for (size_t i = 0; i < max_size(alpha, beta); i++) {
      const T_partials_return alpha_dbl = value_of(alpha_vec[i]);
      const T_partials_return beta_dbl = value_of(beta_vec[i]);

      digamma_alpha_vec[i] = digamma(alpha_dbl);
      digamma_beta_vec[i] = digamma(beta_dbl);
      digamma_sum_vec[i] = digamma(alpha_dbl + beta_dbl);
    }
  }

  for (size_t n = 0; n < N; n++) {
    const T_partials_return y_dbl = value_of(y_vec[n]);
    const T_partials_return alpha_dbl = value_of(alpha_vec[n]);
    const T_partials_return beta_dbl = value_of(beta_vec[n]);
    const T_partials_return betafunc_dbl = exp(lbeta(alpha_dbl, beta_dbl));
    const T_partials_return Pn = inc_beta(alpha_dbl, beta_dbl, y_dbl);

    cdf_log += log(Pn);

    if (!is_constant_struct<T_y>::value)
      ops_partials.edge1_.partials_[n] += pow(1 - y_dbl, beta_dbl - 1)
                                          * pow(y_dbl, alpha_dbl - 1)
                                          / betafunc_dbl / Pn;

    T_partials_return g1 = 0;
    T_partials_return g2 = 0;

    if (contains_nonconstant_struct<T_scale_succ, T_scale_fail>::value) {
      grad_reg_inc_beta(g1, g2, alpha_dbl, beta_dbl, y_dbl,
                        digamma_alpha_vec[n], digamma_beta_vec[n],
                        digamma_sum_vec[n], betafunc_dbl);
    }
    if (!is_constant_struct<T_scale_succ>::value)
      ops_partials.edge2_.partials_[n] += g1 / Pn;
    if (!is_constant_struct<T_scale_fail>::value)
      ops_partials.edge3_.partials_[n] += g2 / Pn;
  }

  return ops_partials.build(cdf_log);
}

}  // namespace math
}  // namespace stan
#endif
