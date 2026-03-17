#ifndef STAN_MATH_PRIM_MAT_FUN_TRANSPOSE_HPP
#define STAN_MATH_PRIM_MAT_FUN_TRANSPOSE_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>

namespace stan {
namespace math {

template <typename T, int R, int C>
Eigen::Matrix<T, C, R> inline transpose(const Eigen::Matrix<T, R, C>& m) {
  return m.transpose();
}

}  // namespace math
}  // namespace stan
#endif
