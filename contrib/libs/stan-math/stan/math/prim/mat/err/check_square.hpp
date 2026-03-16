#ifndef STAN_MATH_PRIM_MAT_ERR_CHECK_SQUARE_HPP
#define STAN_MATH_PRIM_MAT_ERR_CHECK_SQUARE_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/err/check_size_match.hpp>
#include <sstream>

namespace stan {
namespace math {

/**
 * Check if the specified matrix is square. This check allows 0x0 matrices.
 * @tparam T Type of scalar
 * @param function Function name (for error messages)
 * @param name Variable name (for error messages)
 * @param y Matrix to test
 * @throw <code>std::invalid_argument</code> if the matrix is not square
 */
template <typename T_y>
inline void check_square(
    const char* function, const char* name,
    const Eigen::Matrix<T_y, Eigen::Dynamic, Eigen::Dynamic>& y) {
  check_size_match(function, "Expecting a square matrix; rows of ", name,
                   y.rows(), "columns of ", name, y.cols());
}

}  // namespace math
}  // namespace stan
#endif
