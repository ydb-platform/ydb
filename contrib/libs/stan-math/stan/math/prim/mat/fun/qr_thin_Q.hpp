#ifndef STAN_MATH_PRIM_MAT_FUN_QR_THIN_Q_HPP
#define STAN_MATH_PRIM_MAT_FUN_QR_THIN_Q_HPP

#include <stan/math/prim/arr/err/check_nonzero_size.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/err/check_greater_or_equal.hpp>
#include <Eigen/QR>
#include <algorithm>

namespace stan {
namespace math {

/**
 * Returns the orthogonal factor of the thin QR decomposition
 * @param m Matrix.
 * @tparam T scalar type
 * @return Orthogonal matrix with minimal columns
 */
template <typename T>
Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> qr_thin_Q(
    const Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& m) {
  typedef Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> matrix_t;
  check_nonzero_size("qr_thin_Q", "m", m);
  Eigen::HouseholderQR<matrix_t> qr(m.rows(), m.cols());
  qr.compute(m);
  const int min_size = std::min(m.rows(), m.cols());
  matrix_t Q = qr.householderQ() * matrix_t::Identity(m.rows(), min_size);
  for (int i = 0; i < min_size; i++)
    if (qr.matrixQR().coeff(i, i) < 0)
      Q.col(i) *= -1.0;
  return Q;
}

}  // namespace math
}  // namespace stan
#endif
