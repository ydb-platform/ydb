#ifndef STAN_MATH_REV_MAT_FUN_COLUMNS_DOT_SELF_HPP
#define STAN_MATH_REV_MAT_FUN_COLUMNS_DOT_SELF_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/prim/mat/err/check_vector.hpp>
#include <stan/math/rev/core.hpp>
#include <stan/math/rev/mat/fun/typedefs.hpp>
#include <stan/math/rev/mat/fun/dot_self.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Returns the dot product of each column of a matrix with itself.
 * @param x Matrix.
 * @tparam T scalar type
 */
template <int R, int C>
inline Eigen::Matrix<var, 1, C> columns_dot_self(
    const Eigen::Matrix<var, R, C>& x) {
  Eigen::Matrix<var, 1, C> ret(1, x.cols());
  for (size_type i = 0; i < x.cols(); i++) {
    ret(i) = var(new internal::dot_self_vari(x.col(i)));
  }
  return ret;
}

}  // namespace math
}  // namespace stan
#endif
