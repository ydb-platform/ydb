#ifndef STAN_MATH_PRIM_MAT_ERR_CHECK_MULTIPLICABLE_HPP
#define STAN_MATH_PRIM_MAT_ERR_CHECK_MULTIPLICABLE_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>

namespace stan {
namespace math {

/**
 * Check if the matrices can be multiplied.
 * This checks the runtime sizes to determine whether the two
 * matrices are multiplicable. This allows Eigen matrices,
 * vectors, and row vectors to be checked.
 * @tparam T1 Type of first matrix
 * @tparam T2 Type of second matrix
 * @param function Function name (for error messages)
 * @param name1 Variable name for the first matrix (for error messages)
 * @param y1 First matrix to test, requires class access to <code>.rows()</code>
 *   and <code>.cols()</code>
 * @param name2 Variable name for the second matrix (for error messages)
 * @param y2 Second matrix to test, requires class access to
 *   <code>.rows()</code> and <code>.cols()</code>
 * @throw <code>std::invalid_argument</code> if the matrices are not
 *   multiplicable or if either matrix is size 0 for either rows or columns
 */
template <typename T1, typename T2>
inline void check_multiplicable(const char* function, const char* name1,
                                const T1& y1, const char* name2, const T2& y2) {
  check_positive(function, name1, "rows()", y1.rows());
  check_positive(function, name2, "cols()", y2.cols());
  check_size_match(function, "Columns of ", name1, y1.cols(), "Rows of ", name2,
                   y2.rows());
  check_positive(function, name1, "cols()", y1.cols());
}
}  // namespace math
}  // namespace stan
#endif
