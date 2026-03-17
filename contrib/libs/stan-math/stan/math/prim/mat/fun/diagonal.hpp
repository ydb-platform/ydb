#ifndef STAN_MATH_PRIM_MAT_FUN_DIAGONAL_HPP
#define STAN_MATH_PRIM_MAT_FUN_DIAGONAL_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>

namespace stan {
namespace math {

/**
 * Return a column vector of the diagonal elements of the
 * specified matrix.  The matrix is not required to be square.
 * @param m Specified matrix.
 * @return Diagonal of the matrix.
 */
template <typename T>
inline Eigen::Matrix<T, Eigen::Dynamic, 1> diagonal(
    const Eigen::Matrix<T, Eigen::Dynamic, Eigen::Dynamic>& m) {
  return m.diagonal();
}

}  // namespace math
}  // namespace stan
#endif
