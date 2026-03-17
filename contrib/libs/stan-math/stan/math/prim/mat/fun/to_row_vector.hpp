#ifndef STAN_MATH_PRIM_MAT_FUN_TO_ROW_VECTOR_HPP
#define STAN_MATH_PRIM_MAT_FUN_TO_ROW_VECTOR_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
// stan::scalar_type
#include <vector>

namespace stan {
namespace math {

// row_vector to_row_vector(matrix)
// row_vector to_row_vector(vector)
// row_vector to_row_vector(row_vector)
template <typename T, int R, int C>
inline Eigen::Matrix<T, 1, Eigen::Dynamic> to_row_vector(
    const Eigen::Matrix<T, R, C>& matrix) {
  return Eigen::Matrix<T, 1, Eigen::Dynamic>::Map(
      matrix.data(), matrix.rows() * matrix.cols());
}

// row_vector to_row_vector(real[])
template <typename T>
inline Eigen::Matrix<T, 1, Eigen::Dynamic> to_row_vector(
    const std::vector<T>& vec) {
  return Eigen::Matrix<T, 1, Eigen::Dynamic>::Map(vec.data(), vec.size());
}

// row_vector to_row_vector(int[])
inline Eigen::Matrix<double, 1, Eigen::Dynamic> to_row_vector(
    const std::vector<int>& vec) {
  int C = vec.size();
  Eigen::Matrix<double, 1, Eigen::Dynamic> result(C);
  for (int i = 0; i < C; i++)
    result(i) = vec[i];
  return result;
}

}  // namespace math
}  // namespace stan
#endif
