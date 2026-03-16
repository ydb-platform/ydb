#ifndef STAN_MATH_PRIM_MAT_ERR_CHECK_CHOLESKY_FACTOR_CORR_HPP
#define STAN_MATH_PRIM_MAT_ERR_CHECK_CHOLESKY_FACTOR_CORR_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/mat/err/check_lower_triangular.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/prim/mat/err/constraint_tolerance.hpp>
#include <stan/math/prim/mat/err/check_unit_vector.hpp>

namespace stan {
namespace math {

/**
 * Check if the specified matrix is a valid Cholesky factor of a
 * correlation matrix.
 * A Cholesky factor is a lower triangular matrix whose diagonal
 * elements are all positive.  Note that Cholesky factors need not
 * be square, but requires at least as many rows M as columns N
 * (i.e., M &gt;= N).
 * Tolerance is specified by <code>math::CONSTRAINT_TOLERANCE</code>.
 * @tparam T_y Type of elements of Cholesky factor
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param y Matrix to test
 * @throw <code>std::domain_error</code> if y is not a valid Choleksy
 *   factor, if number of rows is less than the number of columns,
 *   if there are 0 columns, or if any element in matrix is NaN
 */
template <typename T_y>
void check_cholesky_factor_corr(
    const char* function, const char* name,
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y) {
  check_square(function, name, y);
  check_lower_triangular(function, name, y);
  for (int i = 0; i < y.rows(); ++i)
    check_positive(function, name, y(i, i));
  for (int i = 0; i < y.rows(); ++i) {
    Eigen::Matrix<T_y, Eigen::Dynamic, 1> y_i = y.row(i).transpose();
    check_unit_vector(function, name, y_i);
  }
}

}  // namespace math
}  // namespace stan
#endif
