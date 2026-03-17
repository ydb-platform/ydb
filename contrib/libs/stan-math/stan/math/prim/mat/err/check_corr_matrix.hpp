#ifndef STAN_MATH_PRIM_MAT_ERR_CHECK_CORR_MATRIX_HPP
#define STAN_MATH_PRIM_MAT_ERR_CHECK_CORR_MATRIX_HPP

#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/mat/err/check_pos_definite.hpp>
#include <stan/math/prim/mat/err/check_symmetric.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <stan/math/prim/mat/err/constraint_tolerance.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/meta/index_type.hpp>
#include <stan/math/prim/scal/meta/error_index.hpp>
#include <sstream>
#include <string>

namespace stan {
namespace math {

/**
 * Check if the specified matrix is a valid correlation matrix.
 * A valid correlation matrix is symmetric, has a unit diagonal
 * (all 1 values), and has all values between -1 and 1
 * (inclusive).
 * This function throws exceptions if the variable is not a valid
 * correlation matrix.
 * @tparam T_y Type of scalar
 * @param function Name of the function this was called from
 * @param name Name of the variable
 * @param y Matrix to test
 * @throw <code>std::invalid_argument</code> if the matrix is not square
 *   or if the matrix is 0x0
 * @throw <code>std::domain_error</code> if the matrix is non-symmetric,
 *   diagonals not near 1, not positive definite, or any of the
 *   elements nan
 */
template <typename T_y>
inline void check_corr_matrix(
    const char* function, const char* name,
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y) {
  typedef typename index_type<
      Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic> >::type size_t;

  check_size_match(function, "Rows of correlation matrix", y.rows(),
                   "columns of correlation matrix", y.cols());
  check_positive(function, name, "rows", y.rows());
  check_symmetric(function, "y", y);

  for (size_t k = 0; k < y.rows(); ++k) {
    if (!(fabs(y(k, k) - 1.0) <= CONSTRAINT_TOLERANCE)) {
      std::ostringstream msg;
      msg << "is not a valid correlation matrix. " << name << "("
          << stan::error_index::value + k << "," << stan::error_index::value + k
          << ") is ";
      std::string msg_str(msg.str());
      domain_error(function, name, y(k, k), msg_str.c_str(),
                   ", but should be near 1.0");
    }
  }
  check_pos_definite(function, "y", y);
}

}  // namespace math
}  // namespace stan
#endif
