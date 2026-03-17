#ifndef STAN_MATH_PRIM_MAT_FUN_QR_THIN_R_HPP
#define STAN_MATH_PRIM_MAT_FUN_QR_THIN_R_HPP

#include <stan/math/prim/arr/err/check_nonzero_size.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/err/check_greater_or_equal.hpp>
#include <Eigen/QR>
#include <algorithm>

namespace stan {
namespace math {

/**
 * Returns the upper triangular factor of the thin QR decomposition
 * @param m Matrix.
 * @tparam T scalar type
 * @return Upper triangular matrix with minimal rows
 */
template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> qr_thin_R(
    const Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& m) {
  typedef Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> matrix_t;
  check_nonzero_size("qr_thin_R", "m", m);
  Eigen::HouseholderQR<matrix_t> qr(m.rows(), m.cols());
  qr.compute(m);
  const int min_size = std::min(m.rows(), m.cols());
  matrix_t R = qr.matrixQR().topLeftCorner(min_size, m.cols());
  for (int i = 0; i < min_size; i++) {
    for (int j = 0; j < i; j++)
      R.coeffRef(i, j) = 0.0;
    if (R(i, i) < 0)
      R.row(i) *= -1.0;
  }
  return R;
}

}  // namespace math
}  // namespace stan
#endif
