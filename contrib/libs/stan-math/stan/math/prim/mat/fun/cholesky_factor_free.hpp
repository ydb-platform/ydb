#ifndef STAN_MATH_PRIM_MAT_FUN_CHOLESKY_FACTOR_FREE_HPP
#define STAN_MATH_PRIM_MAT_FUN_CHOLESKY_FACTOR_FREE_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/err/check_cholesky_factor.hpp>
#include <cmath>
#include <stdexcept>

namespace stan {
namespace math {

/**
 * Return the unconstrained vector of parameters correspdonding to
 * the specified Cholesky factor.  A Cholesky factor must be lower
 * triangular and have positive diagonal elements.
 *
 * @param y Cholesky factor.
 * @return Unconstrained parameters for Cholesky factor.
 * @throw std::domain_error If the matrix is not a Cholesky factor.
 */
template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, 1> cholesky_factor_free(
    const Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& y) {
  using std::log;
  check_cholesky_factor("cholesky_factor_free", "y", y);
  int M = y.rows();
  int N = y.cols();
  Eigen::Matrix<T, Eigen::Dynamic, 1> x((N * (N + 1)) / 2 + (M - N) * N);
  int pos = 0;

  for (int m = 0; m < N; ++m) {
    for (int n = 0; n < m; ++n)
      x(pos++) = y(m, n);
    x(pos++) = log(y(m, m));
  }

  for (int m = N; m < M; ++m)
    for (int n = 0; n < N; ++n)
      x(pos++) = y(m, n);
  return x;
}

}  // namespace math
}  // namespace stan
#endif
