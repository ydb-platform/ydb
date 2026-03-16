#ifndef STAN_MATH_PRIM_MAT_ERR_CHECK_POS_SEMIDEFINITE_HPP
#define STAN_MATH_PRIM_MAT_ERR_CHECK_POS_SEMIDEFINITE_HPP

#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/mat/err/check_symmetric.hpp>
#include <stan/math/prim/mat/err/constraint_tolerance.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/meta/index_type.hpp>
#include <stan/math/prim/mat/fun/value_of_rec.hpp>
#include <sstream>

namespace stan {
namespace math {

/**
 * Check if the specified matrix is positive definite
 * @tparam T_y scalar type of the matrix
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param y Matrix to test
 * @throw <code>std::invalid_argument</code> if the matrix is not square
 *   or if the matrix has 0 size.
 * @throw <code>std::domain_error</code> if the matrix is not symmetric,
 *   or if it is not positive semi-definite,
 *   or if any element of the matrix is <code>NaN</code>.
 */
template <typename T_y>
inline void check_pos_semidefinite(
    const char* function, const char* name,
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y) {
  check_symmetric(function, name, y);
  check_positive(function, name, "rows", y.rows());

  if (y.rows() == 1 && !(y(0, 0) >= 0.0))
    domain_error(function, name, "is not positive semi-definite.", "");

  using Eigen::Dynamic;
  using Eigen::LDLT;
  using Eigen::Matrix;
  LDLT<Matrix<double, Dynamic, Dynamic> > cholesky = value_of_rec(y).ldlt();
  if (cholesky.info() != Eigen::Success
      || (cholesky.vectorD().array() < 0.0).any())
    domain_error(function, name, "is not positive semi-definite.", "");
  check_not_nan(function, name, y);
}

}  // namespace math
}  // namespace stan
#endif
