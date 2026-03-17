#ifndef STAN_MATH_PRIM_MAT_ERR_CHECK_CHOLESKY_FACTOR_HPP
#define STAN_MATH_PRIM_MAT_ERR_CHECK_CHOLESKY_FACTOR_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/err/check_less_or_equal.hpp>
#include <stan/math/prim/mat/err/check_lower_triangular.hpp>

namespace stan {
namespace math {

/**
 * Check if the specified matrix is a valid Cholesky factor.
 * A Cholesky factor is a lower triangular matrix whose diagonal
 * elements are all positive.  Note that Cholesky factors need not
 * be square, but require at least as many rows M as columns N
 * (i.e., M &gt;= N).
 * @tparam T_y Type of elements of Cholesky factor
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param y Matrix to test
 * @throw <code>std::domain_error</code> if y is not a valid Choleksy
 *   factor, if number of rows is less than the number of columns,
 *   if there are 0 columns, or if any element in matrix is NaN
 */
template <typename T_y>
inline void check_cholesky_factor(
    const char* function, const char* name,
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y) {
  check_less_or_equal(function, "columns and rows of Cholesky factor", y.cols(),
                      y.rows());
  check_positive(function, "columns of Cholesky factor", y.cols());
  check_lower_triangular(function, name, y);
  for (int i = 0; i < y.cols(); ++i)
    // FIXME:  should report row
    check_positive(function, name, y(i, i));
}

}  // namespace math
}  // namespace stan
#endif
