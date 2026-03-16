#ifndef STAN_MATH_PRIM_MAT_ERR_CHECK_MATCHING_DIMS_HPP
#define STAN_MATH_PRIM_MAT_ERR_CHECK_MATCHING_DIMS_HPP

#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <stan/math/prim/scal/err/invalid_argument.hpp>
#include <sstream>
#include <string>

namespace stan {
namespace math {

/**
 * Check if the two matrices are of the same size.
 * This function checks the runtime sizes only.
 * @tparam T1 Scalar type of the first matrix
 * @tparam T2 Scalar type of the second matrix
 * @tparam R1 Rows specified at compile time of the first matrix
 * @tparam C1 Columns specified at compile time of the first matrix
 * @tparam R2 Rows specified at compile time of the second matrix
 * @tparam C2 Columns specified at compile time of the second matrix
 * @param function Function name (for error messages)
 * @param name1 Variable name for the first matrix (for error messages)
 * @param y1 First matrix to test
 * @param name2 Variable name for the second matrix (for error messages)
 * @param y2 Second matrix to test
 * @throw <code>std::invalid_argument</code> if the dimensions of the
 *    matrices do not match
 */
template <typename T1, typename T2, int R1, int C1, int R2, int C2>
inline void check_matching_dims(const char* function, const char* name1,
                                const Eigen::Matrix<T1, R1, C1>& y1,
                                const char* name2,
                                const Eigen::Matrix<T2, R2, C2>& y2) {
  check_size_match(function, "Rows of ", name1, y1.rows(), "rows of ", name2,
                   y2.rows());
  check_size_match(function, "Columns of ", name1, y1.cols(), "columns of ",
                   name2, y2.cols());
}

/**
 * Check if the two matrices are of the same size.
 * This function checks the runtime sizes and can also check the static
 * sizes as well. For example, a 4x1 matrix is not the same as a vector
 * with 4 elements.
 * @tparam check_compile Whether to check the static sizes
 * @tparam T1 Scalar type of the first matrix
 * @tparam T2 Scalar type of the second matrix
 * @tparam R1 Rows specified at compile time of the first matrix
 * @tparam C1 Columns specified at compile time of the first matrix
 * @tparam R2 Rows specified at compile time of the second matrix
 * @tparam C2 Columns specified at compile time of the second matrix
 * @param function Function name (for error messages)
 * @param name1 Variable name for the first matrix (for error messages)
 * @param y1 First matrix to test
 * @param name2 Variable name for the second matrix (for error messages)
 * @param y2 Second matrix to test
 * @throw <code>std::invalid_argument</code> if the dimensions of the matrices
 *    do not match
 */
template <bool check_compile, typename T1, typename T2, int R1, int C1, int R2,
          int C2>
inline void check_matching_dims(const char* function, const char* name1,
                                const Eigen::Matrix<T1, R1, C1>& y1,
                                const char* name2,
                                const Eigen::Matrix<T2, R2, C2>& y2) {
  if (check_compile && (R1 != R2 || C1 != C2)) {
    std::ostringstream msg;
    msg << "Static rows and cols of " << name1 << " and " << name2
        << " must match in size.";
    std::string msg_str(msg.str());
    invalid_argument(function, msg_str.c_str(), "", "");
  }
  check_matching_dims(function, name1, y1, name2, y2);
}

}  // namespace math
}  // namespace stan
#endif
