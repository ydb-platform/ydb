#ifndef STAN_MATH_PRIM_MAT_FUN_DIAG_MATRIX_HPP
#define STAN_MATH_PRIM_MAT_FUN_DIAG_MATRIX_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>

namespace stan {
namespace math {

/**
 * Return a square diagonal matrix with the specified vector of
 * coefficients as the diagonal values.
 * @param[in] v Specified vector.
 * @return Diagonal matrix with vector as diagonal values.
 */
template <typename T>
inline Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic> diag_matrix(
    const Eigen::Matrix<T, Eigen::Dynamic, 1>& v) {
  return v.asDiagonal();
}

}  // namespace math
}  // namespace stan
#endif
