#ifndef STAN_MATH_PRIM_MAT_ERR_IS_MAT_FINITE_HPP
#define STAN_MATH_PRIM_MAT_ERR_IS_MAT_FINITE_HPP

#include <Eigen/Dense>
#include <boost/math/special_functions/fpclassify.hpp>

namespace stan {
namespace math {

/**
 * Return <code>true</code> is the specified matrix is finite.
 * @tparam T Scalar type of the matrix, requires class method
 *<code>.allFinite()</code>
 * @tparam R Compile time rows of the matrix
 * @tparam C Compile time columns of the matrix
 * @param y Matrix to test
 * @return <code>true</code> if the matrix is finite
 **/
template <typename T, int R, int C>
inline bool is_mat_finite(const Eigen::Matrix<T, R, C>& y) {
  return y.allFinite();
}

}  // namespace math
}  // namespace stan
#endif
