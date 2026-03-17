#ifndef STAN_MATH_PRIM_MAT_FUN_TO_VECTOR_HPP
#define STAN_MATH_PRIM_MAT_FUN_TO_VECTOR_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
// stan::scalar_type
#include <vector>

namespace stan {
namespace math {

// vector to_vector(matrix)
// vector to_vector(row_vector)
// vector to_vector(vector)
template <typename T, int R, int C>
inline Eigen::Matrix<T, Eigen::Dynamic, 1> to_vector(
    const Eigen::Matrix<T, R, C>& matrix) {
  return Eigen::Matrix<T, Eigen::Dynamic, 1>::Map(
      matrix.data(), matrix.rows() * matrix.cols());
}

// vector to_vector(real[])
template <typename T>
inline Eigen::Matrix<T, Eigen::Dynamic, 1> to_vector(
    const std::vector<T>& vec) {
  return Eigen::Matrix<T, Eigen::Dynamic, 1>::Map(vec.data(), vec.size());
}

// vector to_vector(int[])
inline Eigen::Matrix<double, Eigen::Dynamic, 1> to_vector(
    const std::vector<int>& vec) {
  int R = vec.size();
  Eigen::Matrix<double, Eigen::Dynamic, 1> result(R);
  for (int i = 0; i < R; i++)
    result(i) = vec[i];
  return result;
}

}  // namespace math
}  // namespace stan
#endif
