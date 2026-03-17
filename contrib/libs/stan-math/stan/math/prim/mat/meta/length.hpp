#ifndef STAN_MATH_PRIM_MAT_META_LENGTH_HPP
#define STAN_MATH_PRIM_MAT_META_LENGTH_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>

namespace stan {

/**
 * Returns the size of the provided Eigen matrix.
 *
 * @param m a const Eigen matrix
 * @tparam T type of matrix.
 * @tparam R number of rows in the input matrix.
 * @tparam C number of columns in the input matrix.
 * @return the size of the input matrix
 */
template <typename T, int R, int C>
size_t length(const Eigen::Matrix<T, R, C>& m) {
  return m.size();
}
}  // namespace stan
#endif
