#ifndef STAN_MATH_PRIM_MAT_FUN_DIVIDE_HPP
#define STAN_MATH_PRIM_MAT_FUN_DIVIDE_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <type_traits>

namespace stan {
namespace math {

/**
 * Return specified matrix divided by specified scalar.
 * @tparam R Row type for matrix.
 * @tparam C Column type for matrix.
 * @param m Matrix.
 * @param c Scalar.
 * @return Matrix divided by scalar.
 */
template <int R, int C, typename T>
inline typename std::enable_if<std::is_arithmetic<T>::value,
                               Eigen::Matrix<double, R, C> >::type
divide(const Eigen::Matrix<double, R, C>& m, T c) {
  return m / c;
}

}  // namespace math
}  // namespace stan
#endif
