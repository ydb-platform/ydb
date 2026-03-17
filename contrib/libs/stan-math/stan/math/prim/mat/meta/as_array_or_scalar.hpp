#ifndef STAN_MATH_PRIM_MAT_FUN_AS_ARRAY_OR_SCALAR_HPP
#define STAN_MATH_PRIM_MAT_FUN_AS_ARRAY_OR_SCALAR_HPP

#include <Eigen/Dense>
#include <vector>

namespace stan {
namespace math {

/**
 * Converts a matrix type to an array.
 *
 * @tparam T Type of scalar element.
 * @tparam R Row type of input matrix.
 * @tparam C Column type of input matrix.
 * @param v Specified matrix.
 * @return Matrix converted to an array.
 */
template <typename T, int R, int C>
inline Eigen::ArrayWrapper<const Eigen::Matrix<T, R, C>> as_array_or_scalar(
    const Eigen::Matrix<T, R, C>& v) {
  return v.array();
}

/**
 * Converts a matrix type to an array.
 *
 * @tparam T Type of scalar element.
 * @tparam R Row type of input matrix.
 * @tparam C Column type of input matrix.
 * @param v Specified matrix.
 * @return Matrix converted to an array.
 */
template <typename T, int R, int C>
inline Eigen::ArrayWrapper<const Eigen::Map<const Eigen::Matrix<T, R, C>>>
as_array_or_scalar(const Eigen::Map<const Eigen::Matrix<T, R, C>>& v) {
  return v.array();
}

/**
 * Converts a std::vector type to an array.
 *
 * @tparam T Type of scalar element.
 * @param v Specified vector.
 * @return Matrix converted to an array.
 */
template <typename T>
inline Eigen::Map<const Eigen::Array<T, Eigen::Dynamic, 1>> as_array_or_scalar(
    const std::vector<T>& v) {
  return Eigen::Map<const Eigen::Array<T, Eigen::Dynamic, 1>>(v.data(),
                                                              v.size());
}

}  // namespace math
}  // namespace stan

#endif
