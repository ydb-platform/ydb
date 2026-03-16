#ifndef STAN_MATH_PRIM_MAT_PROB_CATEGORICAL_LPMF_HPP
#define STAN_MATH_PRIM_MAT_PROB_CATEGORICAL_LPMF_HPP

#include <stan/math/prim/mat/err/check_simplex.hpp>
#include <stan/math/prim/scal/err/check_bounded.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/mat/fun/sum.hpp>
#include <stan/math/prim/mat/meta/index_type.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/meta/include_summand.hpp>
#include <stan/math/prim/scal/meta/is_constant_struct.hpp>
#include <boost/random/uniform_01.hpp>
#include <boost/random/variate_generator.hpp>
#include <cmath>
#include <vector>

namespace stan {
namespace math {

// Categorical(n|theta)  [0 < n <= N;   0 <= theta[n] <= 1;  SUM theta = 1]
template <bool propto, typename T_prob>
typename boost::math::tools::promote_args<T_prob>::type categorical_lpmf(
    int n, const Eigen::Matrix<T_prob, Eigen::Dynamic, 1>& theta) {
  static const char* function = "categorical_lpmf";

  using boost::math::tools::promote_args;
  using std::log;

  int lb = 1;

  check_bounded(function, "Number of categories", n, lb, theta.size());
  check_simplex(function, "Probabilities parameter", theta);

  if (include_summand<propto, T_prob>::value)
    return log(theta(n - 1));
  return 0.0;
}

template <typename T_prob>
inline typename boost::math::tools::promote_args<T_prob>::type categorical_lpmf(
    const typename math::index_type<
        Eigen::Matrix<T_prob, Eigen::Dynamic, 1> >::type n,
    const Eigen::Matrix<T_prob, Eigen::Dynamic, 1>& theta) {
  return categorical_lpmf<false>(n, theta);
}

template <bool propto, typename T_prob>
typename boost::math::tools::promote_args<T_prob>::type categorical_lpmf(
    const std::vector<int>& ns,
    const Eigen::Matrix<T_prob, Eigen::Dynamic, 1>& theta) {
  static const char* function = "categorical_lpmf";

  using boost::math::tools::promote_args;
  using std::log;

  int lb = 1;

  for (size_t i = 0; i < ns.size(); ++i)
    check_bounded(function, "element of outcome array", ns[i], lb,
                  theta.size());

  check_simplex(function, "Probabilities parameter", theta);

  if (!include_summand<propto, T_prob>::value)
    return 0.0;

  if (ns.size() == 0)
    return 0.0;

  Eigen::Matrix<T_prob, Eigen::Dynamic, 1> log_theta(theta.size());
  for (int i = 0; i < theta.size(); ++i)
    log_theta(i) = log(theta(i));

  Eigen::Matrix<typename boost::math::tools::promote_args<T_prob>::type,
                Eigen::Dynamic, 1>
      log_theta_ns(ns.size());
  for (size_t i = 0; i < ns.size(); ++i)
    log_theta_ns(i) = log_theta(ns[i] - 1);

  return sum(log_theta_ns);
}

template <typename T_prob>
inline typename boost::math::tools::promote_args<T_prob>::type categorical_lpmf(
    const std::vector<int>& ns,
    const Eigen::Matrix<T_prob, Eigen::Dynamic, 1>& theta) {
  return categorical_lpmf<false>(ns, theta);
}

}  // namespace math
}  // namespace stan
#endif
