#ifndef STAN_MATH_PRIM_MAT_ERR_CHECK_LDLT_FACTOR_HPP
#define STAN_MATH_PRIM_MAT_ERR_CHECK_LDLT_FACTOR_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/mat/fun/LDLT_factor.hpp>
#include <sstream>
#include <string>

namespace stan {
namespace math {

/**
 * Raise domain error if the specified LDLT factor is invalid.  An
 * <code>LDLT_factor</code> is invalid if it was constructed from
 * a matrix that is not positive definite.  The check is that the
 * <code>success()</code> method returns <code>true</code>.
 * @tparam T Type of scalar
 * @tparam R Rows of the matrix
 * @tparam C Columns of the matrix
 * @param[in] function Function name for error messages
 * @param[in] name Variable name for error messages
 * @param[in] A The LDLT factor to check for validity
 * @throws <code>std::domain_error</code> if the LDLT factor is invalid
 */
template <typename T, int R, int C>
inline void check_ldlt_factor(const char* function, const char* name,
                              LDLT_factor<T, R, C>& A) {
  if (!A.success()) {
    std::ostringstream msg;
    msg << "is not positive definite.  last conditional variance is ";
    std::string msg_str(msg.str());
    T too_small = A.vectorD().tail(1)(0);
    domain_error(function, name, too_small, msg_str.c_str(), ".");
  }
}

}  // namespace math
}  // namespace stan
#endif
