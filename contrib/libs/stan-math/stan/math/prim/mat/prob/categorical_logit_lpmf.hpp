#ifndef STAN_MATH_PRIM_MAT_PROB_CATEGORICAL_LOGIT_LPMF_HPP
#define STAN_MATH_PRIM_MAT_PROB_CATEGORICAL_LOGIT_LPMF_HPP

#include <stan/math/prim/scal/err/check_bounded.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/arr/fun/log_sum_exp.hpp>
#include <stan/math/prim/mat/fun/log_softmax.hpp>
#include <stan/math/prim/mat/fun/log_sum_exp.hpp>
#include <stan/math/prim/mat/fun/sum.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <boost/math/tools/promotion.hpp>
#include <vector>

namespace stan {
namespace math {

// CategoricalLog(n|theta)  [0 < n <= N, theta unconstrained], no checking
template <bool propto, typename T_prob>
typename boost::math::tools::promote_args<T_prob>::type categorical_logit_lpmf(
    int n, const Eigen::Matrix<T_prob, Eigen::Dynamic, 1>& beta) {
  static const char* function = "categorical_logit_lpmf";

  check_bounded(function, "categorical outcome out of support", n, 1,
                beta.size());
  check_finite(function, "log odds parameter", beta);

  if (!include_summand<propto, T_prob>::value)
    return 0.0;

  // FIXME:  wasteful vs. creating term (n-1) if not vectorized
  return beta(n - 1) - log_sum_exp(beta);  // == log_softmax(beta)(n-1);
}

template <typename T_prob>
inline typename boost::math::tools::promote_args<T_prob>::type
categorical_logit_lpmf(int n,
                       const Eigen::Matrix<T_prob, Eigen::Dynamic, 1>& beta) {
  return categorical_logit_lpmf<false>(n, beta);
}

template <bool propto, typename T_prob>
typename boost::math::tools::promote_args<T_prob>::type categorical_logit_lpmf(
    const std::vector<int>& ns,
    const Eigen::Matrix<T_prob, Eigen::Dynamic, 1>& beta) {
  static const char* function = "categorical_logit_lpmf";

  for (const auto& x : ns)
    check_bounded(function, "categorical outcome out of support", x, 1,
                  beta.size());
  check_finite(function, "log odds parameter", beta);

  if (!include_summand<propto, T_prob>::value)
    return 0.0;

  if (ns.empty())
    return 0.0;

  Eigen::Matrix<T_prob, Eigen::Dynamic, 1> log_softmax_beta = log_softmax(beta);

  // FIXME:  replace with more efficient sum()
  Eigen::Matrix<typename boost::math::tools::promote_args<T_prob>::type,
                Eigen::Dynamic, 1>
      results(ns.size());
  for (size_t i = 0; i < ns.size(); ++i)
    results[i] = log_softmax_beta(ns[i] - 1);
  return sum(results);
}

template <typename T_prob>
inline typename boost::math::tools::promote_args<T_prob>::type
categorical_logit_lpmf(const std::vector<int>& ns,
                       const Eigen::Matrix<T_prob, Eigen::Dynamic, 1>& beta) {
  return categorical_logit_lpmf<false>(ns, beta);
}

}  // namespace math
}  // namespace stan
#endif
