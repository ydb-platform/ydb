#ifndef STAN_MATH_PRIM_MAT_FUN_CHOLESKY_CORR_FREE_HPP
#define STAN_MATH_PRIM_MAT_FUN_CHOLESKY_CORR_FREE_HPP

#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/fun/corr_constrain.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <stan/math/prim/scal/fun/corr_free.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, 1> cholesky_corr_free(
    const Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& x) {
  using Eigen::Dynamic;
  using Eigen::Matrix;
  using std::sqrt;

  check_square("cholesky_corr_free", "x", x);
  // should validate lower-triangular, unit lengths

  int K = (x.rows() * (x.rows() - 1)) / 2;
  Matrix<T, Dynamic, 1> z(K);
  int k = 0;
  for (int i = 1; i < x.rows(); ++i) {
    z(k++) = corr_free(x(i, 0));
    double sum_sqs = square(x(i, 0));
    for (int j = 1; j < i; ++j) {
      z(k++) = corr_free(x(i, j) / sqrt(1.0 - sum_sqs));
      sum_sqs += square(x(i, j));
    }
  }
  return z;
}

}  // namespace math
}  // namespace stan
#endif
