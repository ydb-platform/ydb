#ifndef STAN_MATH_PRIM_MAT_FUN_AS_COLUMN_VECTOR_OR_SCALAR_HPP
#define STAN_MATH_PRIM_MAT_FUN_AS_COLUMN_VECTOR_OR_SCALAR_HPP

#include <Eigen/Dense>
#include <vector>

namespace stan {
namespace math {

/**
 * Converts input argument to a column vector or a scalar. For column vector
 * inputs this is an identity function.
 *
 * @tparam T Type of scalar element.
 * @param a Specified vector.
 * @return Same vector.
 */
template <typename T>
inline const Eigen::Matrix<T, Eigen::Dynamic, 1>& as_column_vector_or_scalar(
    const Eigen::Matrix<T, Eigen::Dynamic, 1>& a) {
  return a;
}

/**
 * Converts input argument to a column vector or a scalar. For a row vector
 * input this is transpose.
 *
 * @tparam T Type of scalar element.
 * @param a Specified vector.
 * @return Transposed vector.
 */
template <typename T>
inline Eigen::Map<const Eigen::Matrix<T, Eigen::Dynamic, 1>>
as_column_vector_or_scalar(const Eigen::Matrix<T, 1, Eigen::Dynamic>& a) {
  return Eigen::Map<const Eigen::Matrix<T, Eigen::Dynamic, 1>>(
      a.data(), a.size());  // uses Eigen::Map instead of .transpose() so that
                            // there are less possible output types
}

/**
 * Converts input argument to a column vector or a scalar. std::vector will be
 * converted to a column vector.
 *
 * @tparam T Type of scalar element.
 * @param a Specified vector.
 * @return intut converted to a column vector.
 */
template <typename T>
inline Eigen::Map<const Eigen::Matrix<T, Eigen::Dynamic, 1>>
as_column_vector_or_scalar(const std::vector<T>& a) {
  return Eigen::Map<const Eigen::Matrix<T, Eigen::Dynamic, 1>>(a.data(),
                                                               a.size());
}

}  // namespace math
}  // namespace stan

#endif
