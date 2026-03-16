#ifndef STAN_MATH_PRIM_MAT_ERR_IS_SQUARE_HPP
#define STAN_MATH_PRIM_MAT_ERR_IS_SQUARE_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/err/is_size_match.hpp>

namespace stan {
namespace math {

/**
 * Return <code>true</code> if the matrix is square. This check allows 0x0
 * matrices.
 * @tparam T Type of scalar, requires class method <code>.rows()</code>
 *    and <code>.cols()</code>
 * @param y Matrix to test
 * @return <code>true</code> if matrix is square
 */
template <typename T_y>
inline bool is_square(
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y) {
  return is_size_match(y.rows(), y.cols());
}

}  // namespace math
}  // namespace stan
#endif
