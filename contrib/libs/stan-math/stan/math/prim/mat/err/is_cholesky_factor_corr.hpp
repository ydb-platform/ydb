#ifndef STAN_MATH_PRIM_MAT_ERR_IS_CHOLESKY_FACTOR_CORR_HPP
#define STAN_MATH_PRIM_MAT_ERR_IS_CHOLESKY_FACTOR_CORR_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/err/is_cholesky_factor.hpp>
#include <stan/math/prim/mat/err/is_unit_vector.hpp>

namespace stan {
namespace math {
/**
 * Return <code>true</code> if y is a valid Cholesky factor, if
 * the number of rows is not less than the number of columns, if there
 * are no zero columns, and no element in matrix is <code>NaN</code>.
 * A Cholesky factor is a lower triangular matrix whose diagonal
 * elements are all positive. This definition does not require a
 * square matrix.
 * @tparam T_y Type of elements of Cholesky factor, requires class methods
 *   <code>.rows()</code>, <code>.row()</code>, and <code>.transpose()</code>
 * @param y Matrix to test
 * @return <code>true</code> if y is a valid Cholesky factor, if
 *    the number of rows is not less than the number of columns,
 *    if there are no 0 columns, and no element in matrix is <code>NaN</code>
 */
template <typename T_y>
inline bool is_cholesky_factor_corr(
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y) {
  if (!is_cholesky_factor(y))
    return false;
  for (int i = 0; i < y.rows(); ++i) {
    Eigen::Matrix<T_y, Eigen::Dynamic, 1> y_i = y.row(i).transpose();
    if (!is_unit_vector(y_i))
      return false;
  }
  return true;
}

}  // namespace math
}  // namespace stan
#endif
