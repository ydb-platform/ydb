#ifndef STAN_MATH_FWD_MAT_FUN_MULTIPLY_HPP
#define STAN_MATH_FWD_MAT_FUN_MULTIPLY_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/prim/mat/err/check_multiplicable.hpp>
#include <stan/math/fwd/core.hpp>
#include <stan/math/fwd/mat/fun/typedefs.hpp>
#include <stan/math/fwd/mat/fun/to_fvar.hpp>
#include <stan/math/fwd/mat/fun/dot_product.hpp>
#include <boost/math/tools/promotion.hpp>
#include <vector>

namespace stan {
namespace math {

template <typename T, int R1, int C1>
inline Eigen::Matrix<fvar<T>, R1, C1> multiply(
    const Eigen::Matrix<fvar<T>, R1, C1>& m, const fvar<T>& c) {
  Eigen::Matrix<fvar<T>, R1, C1> res(m.rows(), m.cols());
  for (int i = 0; i < m.rows(); i++) {
    for (int j = 0; j < m.cols(); j++)
      res(i, j) = c * m(i, j);
  }
  return res;
}

template <typename T, int R2, int C2>
inline Eigen::Matrix<fvar<T>, R2, C2> multiply(
    const Eigen::Matrix<fvar<T>, R2, C2>& m, double c) {
  Eigen::Matrix<fvar<T>, R2, C2> res(m.rows(), m.cols());
  for (int i = 0; i < m.rows(); i++) {
    for (int j = 0; j < m.cols(); j++)
      res(i, j) = c * m(i, j);
  }
  return res;
}

template <typename T, int R1, int C1>
inline Eigen::Matrix<fvar<T>, R1, C1> multiply(
    const Eigen::Matrix<double, R1, C1>& m, const fvar<T>& c) {
  Eigen::Matrix<fvar<T>, R1, C1> res(m.rows(), m.cols());
  for (int i = 0; i < m.rows(); i++) {
    for (int j = 0; j < m.cols(); j++)
      res(i, j) = c * m(i, j);
  }
  return res;
}

template <typename T, int R1, int C1>
inline Eigen::Matrix<fvar<T>, R1, C1> multiply(
    const fvar<T>& c, const Eigen::Matrix<fvar<T>, R1, C1>& m) {
  return multiply(m, c);
}

template <typename T, int R1, int C1>
inline Eigen::Matrix<fvar<T>, R1, C1> multiply(
    double c, const Eigen::Matrix<fvar<T>, R1, C1>& m) {
  return multiply(m, c);
}

template <typename T, int R1, int C1>
inline Eigen::Matrix<fvar<T>, R1, C1> multiply(
    const fvar<T>& c, const Eigen::Matrix<double, R1, C1>& m) {
  return multiply(m, c);
}

template <typename T, int R1, int C1, int R2, int C2>
inline Eigen::Matrix<fvar<T>, R1, C2> multiply(
    const Eigen::Matrix<fvar<T>, R1, C1>& m1,
    const Eigen::Matrix<fvar<T>, R2, C2>& m2) {
  check_multiplicable("multiply", "m1", m1, "m2", m2);
  Eigen::Matrix<fvar<T>, R1, C2> result(m1.rows(), m2.cols());
  for (size_type i = 0; i < m1.rows(); i++) {
    Eigen::Matrix<fvar<T>, 1, C1> crow = m1.row(i);
    for (size_type j = 0; j < m2.cols(); j++) {
      Eigen::Matrix<fvar<T>, R2, 1> ccol = m2.col(j);
      result(i, j) = dot_product(crow, ccol);
    }
  }
  return result;
}

template <typename T, int R1, int C1, int R2, int C2>
inline Eigen::Matrix<fvar<T>, R1, C2> multiply(
    const Eigen::Matrix<fvar<T>, R1, C1>& m1,
    const Eigen::Matrix<double, R2, C2>& m2) {
  check_multiplicable("multiply", "m1", m1, "m2", m2);
  Eigen::Matrix<fvar<T>, R1, C2> result(m1.rows(), m2.cols());
  for (size_type i = 0; i < m1.rows(); i++) {
    Eigen::Matrix<fvar<T>, 1, C1> crow = m1.row(i);
    for (size_type j = 0; j < m2.cols(); j++) {
      Eigen::Matrix<double, R2, 1> ccol = m2.col(j);
      result(i, j) = dot_product(crow, ccol);
    }
  }
  return result;
}

template <typename T, int R1, int C1, int R2, int C2>
inline Eigen::Matrix<fvar<T>, R1, C2> multiply(
    const Eigen::Matrix<double, R1, C1>& m1,
    const Eigen::Matrix<fvar<T>, R2, C2>& m2) {
  check_multiplicable("multiply", "m1", m1, "m2", m2);
  Eigen::Matrix<fvar<T>, R1, C2> result(m1.rows(), m2.cols());
  for (size_type i = 0; i < m1.rows(); i++) {
    Eigen::Matrix<double, 1, C1> crow = m1.row(i);
    for (size_type j = 0; j < m2.cols(); j++) {
      Eigen::Matrix<fvar<T>, R2, 1> ccol = m2.col(j);
      result(i, j) = dot_product(crow, ccol);
    }
  }
  return result;
}

template <typename T, int C1, int R2>
inline fvar<T> multiply(const Eigen::Matrix<fvar<T>, 1, C1>& rv,
                        const Eigen::Matrix<fvar<T>, R2, 1>& v) {
  check_multiplicable("multiply", "rv", rv, "v", v);
  return dot_product(rv, v);
}

template <typename T, int C1, int R2>
inline fvar<T> multiply(const Eigen::Matrix<fvar<T>, 1, C1>& rv,
                        const Eigen::Matrix<double, R2, 1>& v) {
  check_multiplicable("multiply", "rv", rv, "v", v);
  return dot_product(rv, v);
}

template <typename T, int C1, int R2>
inline fvar<T> multiply(const Eigen::Matrix<double, 1, C1>& rv,
                        const Eigen::Matrix<fvar<T>, R2, 1>& v) {
  check_multiplicable("multiply", "rv", rv, "v", v);
  return dot_product(rv, v);
}

}  // namespace math
}  // namespace stan
#endif
