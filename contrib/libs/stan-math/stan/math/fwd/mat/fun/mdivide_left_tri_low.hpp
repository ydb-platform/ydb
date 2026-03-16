#ifndef STAN_MATH_FWD_MAT_FUN_MDIVIDE_LEFT_TRI_LOW_HPP
#define STAN_MATH_FWD_MAT_FUN_MDIVIDE_LEFT_TRI_LOW_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/prim/mat/fun/mdivide_left.hpp>
#include <stan/math/prim/mat/err/check_multiplicable.hpp>
#include <stan/math/fwd/mat/fun/typedefs.hpp>
#include <stan/math/fwd/mat/fun/to_fvar.hpp>
#include <stan/math/fwd/mat/fun/multiply.hpp>
#include <stan/math/fwd/mat/fun/inverse.hpp>
#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <vector>

namespace stan {
namespace math {

template <typename T, int R1, int C1, int R2, int C2>
inline Eigen::Matrix<fvar<T>, R1, C1> mdivide_left_tri_low(
    const Eigen::Matrix<fvar<T>, R1, C1>& A,
    const Eigen::Matrix<fvar<T>, R2, C2>& b) {
  check_square("mdivide_left_tri_low", "A", A);
  check_multiplicable("mdivide_left_tri_low", "A", A, "b", b);

  Eigen::Matrix<T, R1, C2> inv_A_mult_b(A.rows(), b.cols());
  Eigen::Matrix<T, R1, C2> inv_A_mult_deriv_b(A.rows(), b.cols());
  Eigen::Matrix<T, R1, C1> inv_A_mult_deriv_A(A.rows(), A.cols());
  Eigen::Matrix<T, R1, C1> val_A(A.rows(), A.cols());
  Eigen::Matrix<T, R1, C1> deriv_A(A.rows(), A.cols());
  Eigen::Matrix<T, R2, C2> val_b(b.rows(), b.cols());
  Eigen::Matrix<T, R2, C2> deriv_b(b.rows(), b.cols());
  val_A.setZero();
  deriv_A.setZero();

  for (size_type j = 0; j < A.cols(); j++) {
    for (size_type i = j; i < A.rows(); i++) {
      val_A(i, j) = A(i, j).val_;
      deriv_A(i, j) = A(i, j).d_;
    }
  }

  for (size_type j = 0; j < b.cols(); j++) {
    for (size_type i = 0; i < b.rows(); i++) {
      val_b(i, j) = b(i, j).val_;
      deriv_b(i, j) = b(i, j).d_;
    }
  }

  inv_A_mult_b = mdivide_left(val_A, val_b);
  inv_A_mult_deriv_b = mdivide_left(val_A, deriv_b);
  inv_A_mult_deriv_A = mdivide_left(val_A, deriv_A);

  Eigen::Matrix<T, R1, C2> deriv(A.rows(), b.cols());
  deriv = inv_A_mult_deriv_b - multiply(inv_A_mult_deriv_A, inv_A_mult_b);

  return to_fvar(inv_A_mult_b, deriv);
}

template <typename T, int R1, int C1, int R2, int C2>
inline Eigen::Matrix<fvar<T>, R1, C1> mdivide_left_tri_low(
    const Eigen::Matrix<double, R1, C1>& A,
    const Eigen::Matrix<fvar<T>, R2, C2>& b) {
  check_square("mdivide_left_tri_low", "A", A);
  check_multiplicable("mdivide_left_tri_low", "A", A, "b", b);

  Eigen::Matrix<T, R1, C2> inv_A_mult_b(A.rows(), b.cols());
  Eigen::Matrix<T, R1, C2> inv_A_mult_deriv_b(A.rows(), b.cols());
  Eigen::Matrix<T, R2, C2> val_b(b.rows(), b.cols());
  Eigen::Matrix<T, R2, C2> deriv_b(b.rows(), b.cols());
  Eigen::Matrix<double, R1, C1> val_A(A.rows(), A.cols());
  val_A.setZero();

  for (size_type j = 0; j < A.cols(); j++) {
    for (size_type i = j; i < A.rows(); i++) {
      val_A(i, j) = A(i, j);
    }
  }

  for (size_type j = 0; j < b.cols(); j++) {
    for (size_type i = 0; i < b.rows(); i++) {
      val_b(i, j) = b(i, j).val_;
      deriv_b(i, j) = b(i, j).d_;
    }
  }

  inv_A_mult_b = mdivide_left(val_A, val_b);
  inv_A_mult_deriv_b = mdivide_left(val_A, deriv_b);

  Eigen::Matrix<T, R1, C2> deriv(A.rows(), b.cols());
  deriv = inv_A_mult_deriv_b;

  return to_fvar(inv_A_mult_b, deriv);
}

template <typename T, int R1, int C1, int R2, int C2>
inline Eigen::Matrix<fvar<T>, R1, C1> mdivide_left_tri_low(
    const Eigen::Matrix<fvar<T>, R1, C1>& A,
    const Eigen::Matrix<double, R2, C2>& b) {
  check_square("mdivide_left_tri_low", "A", A);
  check_multiplicable("mdivide_left_tri_low", "A", A, "b", b);

  Eigen::Matrix<T, R1, C2> inv_A_mult_b(A.rows(), b.cols());
  Eigen::Matrix<T, R1, C1> inv_A_mult_deriv_A(A.rows(), A.cols());
  Eigen::Matrix<T, R1, C1> val_A(A.rows(), A.cols());
  Eigen::Matrix<T, R1, C1> deriv_A(A.rows(), A.cols());
  val_A.setZero();
  deriv_A.setZero();

  for (size_type j = 0; j < A.cols(); j++) {
    for (size_type i = j; i < A.rows(); i++) {
      val_A(i, j) = A(i, j).val_;
      deriv_A(i, j) = A(i, j).d_;
    }
  }

  inv_A_mult_b = mdivide_left(val_A, b);
  inv_A_mult_deriv_A = mdivide_left(val_A, deriv_A);

  Eigen::Matrix<T, R1, C2> deriv(A.rows(), b.cols());
  deriv = -multiply(inv_A_mult_deriv_A, inv_A_mult_b);

  return to_fvar(inv_A_mult_b, deriv);
}

}  // namespace math
}  // namespace stan
#endif
