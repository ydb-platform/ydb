#ifndef STAN_MATH_PRIM_MAT_ERR_CHECK_COLUMN_INDEX_HPP
#define STAN_MATH_PRIM_MAT_ERR_CHECK_COLUMN_INDEX_HPP

#include <stan/math/prim/scal/err/out_of_range.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/meta/error_index.hpp>
#include <sstream>
#include <string>

namespace stan {
namespace math {

/**
 * Check if the specified index is a valid column of the matrix.
 * By default this is a 1-indexed check (as opposed to
 * 0-indexed). Behavior can be changed by setting
 * <code>stan::error_index::value</code>. This function will
 * throw an <code>std::out_of_range</code> exception if
 * the index is out of bounds.
 * @tparam T_y Type of scalar
 * @tparam R Number of rows of the matrix
 * @tparam C Number of columns of the matrix
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param y Matrix to test
 * @param i Index to check
 * @throw <code>std::out_of_range</code> if index is an invalid column
 */
template <typename T_y, int R, int C>
inline void check_column_index(const char* function, const char* name,
                               const Eigen::Matrix<T_y, R, C>& y, size_t i) {
  if (i >= stan::error_index::value
      && i < static_cast<size_t>(y.cols()) + stan::error_index::value)
    return;

  std::stringstream msg;
  msg << " for columns of " << name;
  std::string msg_str(msg.str());
  out_of_range(function, y.cols(), i, msg_str.c_str());
}

}  // namespace math
}  // namespace stan
#endif
