#ifndef STAN_MATH_PRIM_MAT_FUN_SUB_ROW_HPP
#define STAN_MATH_PRIM_MAT_FUN_SUB_ROW_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/err/check_row_index.hpp>
#include <stan/math/prim/mat/err/check_column_index.hpp>

namespace stan {
namespace math {

/**
 * Return a 1 x nrows subrow starting at (i-1, j-1).
 *
 * @param m Matrix Input matrix.
 * @param i Starting row + 1.
 * @param j Starting column + 1.
 * @param ncols Number of columns in block.
 * @throw std::out_of_range if either index is out of range.
 */
template <typename T>
inline Eigen::Matrix<T, 1, Eigen::Dynamic> sub_row(
    const Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& m, size_t i,
    size_t j, size_t ncols) {
  check_row_index("sub_row", "i", m, i);
  check_column_index("sub_row", "j", m, j);
  if (ncols > 0)
    check_column_index("sub_col", "j+ncols-1", m, j + ncols - 1);
  return m.block(i - 1, j - 1, 1, ncols);
}

}  // namespace math
}  // namespace stan
#endif
