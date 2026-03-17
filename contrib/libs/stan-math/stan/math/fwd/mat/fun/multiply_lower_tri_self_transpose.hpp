#ifndef STAN_MATH_FWD_MAT_FUN_MULTIPLY_LOWER_TRI_SELF_TRANSPOSE_HPP
#define STAN_MATH_FWD_MAT_FUN_MULTIPLY_LOWER_TRI_SELF_TRANSPOSE_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/fwd/mat/fun/typedefs.hpp>
#include <stan/math/fwd/mat/fun/multiply.hpp>
#include <stan/math/prim/mat/fun/transpose.hpp>
#include <vector>

namespace stan {
namespace math {

template <typename T, int R, int C>
inline Eigen::Matrix<fvar<T>, R, R> multiply_lower_tri_self_transpose(
    const Eigen::Matrix<fvar<T>, R, C>& m) {
  if (m.rows() == 0)
    return Eigen::Matrix<fvar<T>, R, R>(0, 0);
  Eigen::Matrix<fvar<T>, R, C> L(m.rows(), m.cols());
  L.setZero();

  for (size_type i = 0; i < m.rows(); i++) {
    for (size_type j = 0; (j < i + 1) && (j < m.cols()); j++)
      L(i, j) = m(i, j);
  }
  return multiply(L, transpose(L));
}

}  // namespace math
}  // namespace stan
#endif
