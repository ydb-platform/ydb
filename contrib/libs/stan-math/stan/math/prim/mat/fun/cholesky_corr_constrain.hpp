#ifndef STAN_MATH_PRIM_MAT_FUN_CHOLESKY_CORR_CONSTRAIN_HPP
#define STAN_MATH_PRIM_MAT_FUN_CHOLESKY_CORR_CONSTRAIN_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <stan/math/prim/scal/fun/log1m.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <stan/math/prim/scal/fun/corr_constrain.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> cholesky_corr_constrain(
    const Eigen::Matrix<T, Eigen::Dynamic, 1>& y, int K) {
  using Eigen::Dynamic;
  using Eigen::Matrix;
  using std::sqrt;
  int k_choose_2 = (K * (K - 1)) / 2;
  check_size_match("cholesky_corr_constrain", "y.size()", y.size(),
                   "k_choose_2", k_choose_2);
  Matrix<T, Dynamic, 1> z(k_choose_2);
  for (int i = 0; i < k_choose_2; ++i)
    z(i) = corr_constrain(y(i));
  Matrix<T, Dynamic, Dynamic> x(K, K);
  if (K == 0)
    return x;
  x.setZero();
  x(0, 0) = 1;
  int k = 0;
  for (int i = 1; i < K; ++i) {
    x(i, 0) = z(k++);
    T sum_sqs(square(x(i, 0)));
    for (int j = 1; j < i; ++j) {
      x(i, j) = z(k++) * sqrt(1.0 - sum_sqs);
      sum_sqs += square(x(i, j));
    }
    x(i, i) = sqrt(1.0 - sum_sqs);
  }
  return x;
}

// FIXME to match above after debugged
template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> cholesky_corr_constrain(
    const Eigen::Matrix<T, Eigen::Dynamic, 1>& y, int K, T& lp) {
  using Eigen::Dynamic;
  using Eigen::Matrix;
  using std::sqrt;
  int k_choose_2 = (K * (K - 1)) / 2;
  check_size_match("cholesky_corr_constrain", "y.size()", y.size(),
                   "k_choose_2", k_choose_2);
  Matrix<T, Dynamic, 1> z(k_choose_2);
  for (int i = 0; i < k_choose_2; ++i)
    z(i) = corr_constrain(y(i), lp);
  Matrix<T, Dynamic, Dynamic> x(K, K);
  if (K == 0)
    return x;
  x.setZero();
  x(0, 0) = 1;
  int k = 0;
  for (int i = 1; i < K; ++i) {
    x(i, 0) = z(k++);
    T sum_sqs = square(x(i, 0));
    for (int j = 1; j < i; ++j) {
      lp += 0.5 * log1m(sum_sqs);
      x(i, j) = z(k++) * sqrt(1.0 - sum_sqs);
      sum_sqs += square(x(i, j));
    }
    x(i, i) = sqrt(1.0 - sum_sqs);
  }
  return x;
}

}  // namespace math
}  // namespace stan
#endif
