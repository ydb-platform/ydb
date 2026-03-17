#ifndef STAN_MATH_PRIM_MAT_ERR_CHECK_COV_MATRIX_HPP
#define STAN_MATH_PRIM_MAT_ERR_CHECK_COV_MATRIX_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/err/check_pos_definite.hpp>

namespace stan {
namespace math {
/**
 * Check if the specified matrix is a valid covariance matrix.
 * A valid covariance matrix is a square, symmetric matrix that is
 * positive definite.
 * @tparam T Type of scalar.
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param y Matrix to test
 * @throw <code>std::invalid_argument</code> if the matrix is not square
 *   or if the matrix is 0x0
 * @throw <code>std::domain_error</code> if the matrix is not symmetric,
 *   if the matrix is not positive definite,
 *   or if any element of the matrix is nan
 */
template <typename T_y>
inline void check_cov_matrix(
    const char* function, const char* name,
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y) {
  check_pos_definite(function, name, y);
}

}  // namespace math
}  // namespace stan
#endif
