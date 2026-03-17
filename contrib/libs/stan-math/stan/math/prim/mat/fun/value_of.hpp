#ifndef STAN_MATH_PRIM_MAT_FUN_VALUE_OF_HPP
#define STAN_MATH_PRIM_MAT_FUN_VALUE_OF_HPP

#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/prim/scal/meta/child_type.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>

namespace stan {
namespace math {

/**
 * Convert a matrix of type T to a matrix of doubles.
 *
 * T must implement value_of. See
 * test/math/fwd/mat/fun/value_of.cpp for fvar and var usage.
 *
 * @tparam T Scalar type in matrix
 * @tparam R Rows of matrix
 * @tparam C Columns of matrix
 * @param[in] M Matrix to be converted
 * @return Matrix of values
 **/
template <typename T, int R, int C>
inline Eigen::Matrix<typename child_type<T>::type, R, C> value_of(
    const Eigen::Matrix<T, R, C>& M) {
  Eigen::Matrix<typename child_type<T>::type, R, C> Md(M.rows(), M.cols());
  for (int j = 0; j < M.cols(); j++)
    for (int i = 0; i < M.rows(); i++)
      Md(i, j) = value_of(M(i, j));
  return Md;
}

/**
 * Return the specified argument.
 *
 * <p>See <code>value_of(T)</code> for a polymorphic
 * implementation using static casts.
 *
 * <p>This inline pass-through no-op should be compiled away.
 *
 * @param x Specified matrix.
 * @return Specified matrix.
 */
template <int R, int C>
inline const Eigen::Matrix<double, R, C>& value_of(
    const Eigen::Matrix<double, R, C>& x) {
  return x;
}
}  // namespace math
}  // namespace stan

#endif
