#ifndef STAN_MATH_PRIM_MAT_ERR_IS_COLUMN_INDEX_HPP
#define STAN_MATH_PRIM_MAT_ERR_IS_COLUMN_INDEX_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/meta/error_index.hpp>

namespace stan {
namespace math {

/**
 * Return <code>true</code> no index is invalid column.
 * By default this is a 1-indexed check (as opposed to zero-indexed).
 * Behavior can be changed by setting <code>stan::error_index::value</code>.
 * @tparam T_y Type of scalar, requires class method <code>.cols()</code>
 * @tparam R Number of rows of the matrix
 * @tparam C Number of columns of the matrix
 * @param y Matrix to test
 * @param i Index to check
 * @return <code>true</code> no index is invalid column
 */
template <typename T_y, int R, int C>
inline bool is_column_index(const Eigen::Matrix<T_y, R, C>& y, size_t i) {
  return i >= stan::error_index::value
         && i < static_cast<size_t>(y.cols()) + stan::error_index::value;
}

}  // namespace math
}  // namespace stan
#endif
