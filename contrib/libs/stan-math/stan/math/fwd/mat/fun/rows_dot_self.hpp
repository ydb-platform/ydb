#ifndef STAN_MATH_FWD_MAT_FUN_ROWS_DOT_SELF_HPP
#define STAN_MATH_FWD_MAT_FUN_ROWS_DOT_SELF_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/fwd/core.hpp>
#include <stan/math/fwd/mat/fun/dot_self.hpp>
#include <vector>

namespace stan {
namespace math {

template <typename T, int R, int C>
inline Eigen::Matrix<fvar<T>, R, 1> rows_dot_self(
    const Eigen::Matrix<fvar<T>, R, C>& x) {
  Eigen::Matrix<fvar<T>, R, 1> ret(x.rows(), 1);
  for (size_type i = 0; i < x.rows(); i++) {
    Eigen::Matrix<fvar<T>, 1, C> crow = x.row(i);
    ret(i, 0) = dot_self(crow);
  }
  return ret;
}
}  // namespace math
}  // namespace stan
#endif
