#ifndef STAN_MATH_PRIM_MAT_FUN_MULTIPLY_HPP
#define STAN_MATH_PRIM_MAT_FUN_MULTIPLY_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/arr/err/check_matching_sizes.hpp>
#include <stan/math/prim/mat/err/check_multiplicable.hpp>
#include <type_traits>

namespace stan {
namespace math {

/**
 * Return specified matrix multiplied by specified scalar.
 * @tparam R Row type for matrix.
 * @tparam C Column type for matrix.
 * @param m Matrix.
 * @param c Scalar.
 * @return Product of matrix and scalar.
 */
template <int R, int C, typename T>
inline typename std::enable_if<std::is_arithmetic<T>::value,
                               Eigen::Matrix<double, R, C> >::type
multiply(const Eigen::Matrix<double, R, C>& m, T c) {
  return c * m;
}

/**
 * Return specified scalar multiplied by specified matrix.
 * @tparam R Row type for matrix.
 * @tparam C Column type for matrix.
 * @param c Scalar.
 * @param m Matrix.
 * @return Product of scalar and matrix.
 */
template <int R, int C, typename T>
inline typename std::enable_if<std::is_arithmetic<T>::value,
                               Eigen::Matrix<double, R, C> >::type
multiply(T c, const Eigen::Matrix<double, R, C>& m) {
  return c * m;
}

/**
 * Return the product of the specified matrices.  The number of
 * columns in the first matrix must be the same as the number of rows
 * in the second matrix.
 * @param m1 First matrix.
 * @param m2 Second matrix.
 * @return The product of the first and second matrices.
 * @throw std::domain_error if the number of columns of m1 does not match
 *   the number of rows of m2.
 */
template <int R1, int C1, int R2, int C2>
inline Eigen::Matrix<double, R1, C2> multiply(
    const Eigen::Matrix<double, R1, C1>& m1,
    const Eigen::Matrix<double, R2, C2>& m2) {
  check_multiplicable("multiply", "m1", m1, "m2", m2);
  return m1 * m2;
}

/**
 * Return the scalar product of the specified row vector and
 * specified column vector.  The return is the same as the dot
 * product.  The two vectors must be the same size.
 * @param rv Row vector.
 * @param v Column vector.
 * @return Scalar result of multiplying row vector by column vector.
 * @throw std::domain_error if rv and v are not the same size.
 */
template <int C1, int R2>
inline double multiply(const Eigen::Matrix<double, 1, C1>& rv,
                       const Eigen::Matrix<double, R2, 1>& v) {
  check_matching_sizes("multiply", "rv", rv, "v", v);
  return rv.dot(v);
}

}  // namespace math
}  // namespace stan
#endif
