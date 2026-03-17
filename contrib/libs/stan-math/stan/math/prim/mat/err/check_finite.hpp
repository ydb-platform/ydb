#ifndef STAN_MATH_PRIM_MAT_ERR_CHECK_FINITE_HPP
#define STAN_MATH_PRIM_MAT_ERR_CHECK_FINITE_HPP

#include <stan/math/prim/scal/err/domain_error.hpp>
#include <stan/math/prim/scal/err/check_finite.hpp>
#include <stan/math/prim/mat/fun/value_of.hpp>
#include <Eigen/Dense>
#include <boost/math/special_functions/fpclassify.hpp>

namespace stan {
namespace math {

/*
 * Return <code>true</code> is the specified matrix is finite.
 * @tparams T Scalar type of the matrix, requires class method
 *   <code>.size()</code>
 * @tparams R Compile time rows of the matrix
 * @tparams C Compile time columns of the matrix
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param y Matrix to test
 * @return <code>true</code> if the matrix is finite
 **/
namespace internal {
template <typename T, int R, int C>
struct finite<Eigen::Matrix<T, R, C>, true> {
  static void check(const char* function, const char* name,
                    const Eigen::Matrix<T, R, C>& y) {
    if (!value_of(y).allFinite()) {
      for (int n = 0; n < y.size(); ++n) {
        if (!(boost::math::isfinite)(y(n)))
          domain_error_vec(function, name, y, n, "is ",
                           ", but must be finite!");
      }
    }
  }
};
}  // namespace internal
}  // namespace math
}  // namespace stan

#endif
