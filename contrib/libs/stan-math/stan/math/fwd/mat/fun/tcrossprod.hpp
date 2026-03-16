#ifndef STAN_MATH_FWD_MAT_FUN_TCROSSPROD_HPP
#define STAN_MATH_FWD_MAT_FUN_TCROSSPROD_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/fwd/mat/fun/typedefs.hpp>
#include <stan/math/fwd/mat/fun/multiply.hpp>
#include <stan/math/prim/mat/fun/transpose.hpp>
#include <vector>

namespace stan {
namespace math {

template <typename T, int R, int C>
inline Eigen::Matrix<fvar<T>, R, R> tcrossprod(
    const Eigen::Matrix<fvar<T>, R, C>& m) {
  if (m.rows() == 0)
    return Eigen::Matrix<fvar<T>, R, R>(0, 0);
  return multiply(m, transpose(m));
}

}  // namespace math
}  // namespace stan
#endif
