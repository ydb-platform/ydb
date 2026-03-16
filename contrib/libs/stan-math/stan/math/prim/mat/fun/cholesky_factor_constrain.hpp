#ifndef STAN_MATH_PRIM_MAT_FUN_CHOLESKY_FACTOR_CONSTRAIN_HPP
#define STAN_MATH_PRIM_MAT_FUN_CHOLESKY_FACTOR_CONSTRAIN_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/sum.hpp>
#include <stan/math/prim/scal/err/check_greater_or_equal.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <cmath>
#include <stdexcept>
#include <vector>

namespace stan {
namespace math {

/**
 * Return the Cholesky factor of the specified size read from the
 * specified vector.  A total of (N choose 2) + N + (M - N) * N
 * elements are required to read an M by N Cholesky factor.
 *
 * @tparam T Type of scalars in matrix
 * @param x Vector of unconstrained values
 * @param M Number of rows
 * @param N Number of columns
 * @return Cholesky factor
 */
template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> cholesky_factor_constrain(
    const Eigen::Matrix<T, Eigen::Dynamic, 1>& x, int M, int N) {
  using std::exp;
  check_greater_or_equal("cholesky_factor_constrain",
                         "num rows (must be greater or equal to num cols)", M,
                         N);
  check_size_match("cholesky_factor_constrain", "x.size()", x.size(),
                   "((N * (N + 1)) / 2 + (M - N) * N)",
                   ((N * (N + 1)) / 2 + (M - N) * N));
  Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> y(M, N);
  T zero(0);
  int pos = 0;

  for (int m = 0; m < N; ++m) {
    for (int n = 0; n < m; ++n)
      y(m, n) = x(pos++);
    y(m, m) = exp(x(pos++));
    for (int n = m + 1; n < N; ++n)
      y(m, n) = zero;
  }

  for (int m = N; m < M; ++m)
    for (int n = 0; n < N; ++n)
      y(m, n) = x(pos++);
  return y;
}

/**
 * Return the Cholesky factor of the specified size read from the
 * specified vector and increment the specified log probability
 * reference with the log Jacobian adjustment of the transform.  A total
 * of (N choose 2) + N + N * (M - N) free parameters are required to read
 * an M by N Cholesky factor.
 *
 * @tparam T Type of scalars in matrix
 * @param x Vector of unconstrained values
 * @param M Number of rows
 * @param N Number of columns
 * @param lp Log probability that is incremented with the log Jacobian
 * @return Cholesky factor
 */
template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> cholesky_factor_constrain(
    const Eigen::Matrix<T, Eigen::Dynamic, 1>& x, int M, int N, T& lp) {
  check_size_match("cholesky_factor_constrain", "x.size()", x.size(),
                   "((N * (N + 1)) / 2 + (M - N) * N)",
                   ((N * (N + 1)) / 2 + (M - N) * N));
  int pos = 0;
  std::vector<T> log_jacobians(N);
  for (int n = 0; n < N; ++n) {
    pos += n;
    log_jacobians[n] = x(pos++);
  }
  lp += sum(log_jacobians);
  return cholesky_factor_constrain(x, M, N);
}

}  // namespace math
}  // namespace stan
#endif
