#ifndef STAN_MATH_PRIM_MAT_ERR_CHECK_SYMMETRIC_HPP
#define STAN_MATH_PRIM_MAT_ERR_CHECK_SYMMETRIC_HPP

#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/mat/err/check_square.hpp>
#include <stan/math/prim/mat/err/constraint_tolerance.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/meta/index_type.hpp>
#include <stan/math/prim/mat/fun/value_of.hpp>
#include <stan/math/prim/scal/meta/error_index.hpp>
#include <sstream>
#include <string>

namespace stan {
namespace math {

/**
 * Check if the specified matrix is symmetric.
 * The error message is either 0 or 1 indexed, specified by
 * <code>stan::error_index::value</code>.
 * @tparam T_y Type of scalar
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param y Matrix to test
 * @throw <code>std::invalid_argument</code> if the matrix is not square.
 * @throw <code>std::domain_error</code> if any element not on the
 *   main diagonal is <code>NaN</code>
 */
template <typename T_y>
inline void check_symmetric(
    const char* function, const char* name,
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y) {
  check_square(function, name, y);

  typedef typename index_type<
      Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic> >::type size_type;

  size_type k = y.rows();
  if (k == 1)
    return;
  for (size_type m = 0; m < k; ++m) {
    for (size_type n = m + 1; n < k; ++n) {
      if (!(fabs(value_of(y(m, n)) - value_of(y(n, m)))
            <= CONSTRAINT_TOLERANCE)) {
        std::ostringstream msg1;
        msg1 << "is not symmetric. " << name << "["
             << stan::error_index::value + m << ","
             << stan::error_index::value + n << "] = ";
        std::string msg1_str(msg1.str());
        std::ostringstream msg2;
        msg2 << ", but " << name << "[" << stan::error_index::value + n << ","
             << stan::error_index::value + m << "] = " << y(n, m);
        std::string msg2_str(msg2.str());
        domain_error(function, name, y(m, n), msg1_str.c_str(),
                     msg2_str.c_str());
      }
    }
  }
}

}  // namespace math
}  // namespace stan
#endif
