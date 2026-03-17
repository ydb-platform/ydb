#ifndef STAN_MATH_FWD_MAT_FUN_QR_R_HPP
#define STAN_MATH_FWD_MAT_FUN_QR_R_HPP

#include <stan/math/prim/arr/err/check_nonzero_size.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/err/check_greater_or_equal.hpp>
#include <stan/math/fwd/core.hpp>
#include <Eigen/QR>

namespace stan {
namespace math {

template <typename T>
Eigen::Matrix<fvar<T>, Eigen::Dynamic, Eigen::Dynamic> qr_R(
    const Eigen::Matrix<fvar<T>, Eigen::Dynamic, Eigen::Dynamic>& m) {
  typedef Eigen::Matrix<fvar<T>, Eigen::Dynamic, Eigen::Dynamic> matrix_fwd_t;
  check_nonzero_size("qr_R", "m", m);
  check_greater_or_equal("qr_R", "m.rows()", m.rows(), m.cols());
  Eigen::HouseholderQR<matrix_fwd_t> qr(m.rows(), m.cols());
  qr.compute(m);
  matrix_fwd_t R = qr.matrixQR().topLeftCorner(m.rows(), m.cols());
  for (int i = 0; i < R.rows(); i++) {
    for (int j = 0; j < i; j++)
      R(i, j) = 0.0;
    if (i < R.cols() && R(i, i) < 0.0)
      R.row(i) *= -1.0;
  }
  return R;
}

}  // namespace math
}  // namespace stan
#endif
