#ifndef STAN_MATH_PRIM_MAT_FUN_MIN_HPP
#define STAN_MATH_PRIM_MAT_FUN_MIN_HPP

#include <stan/math/prim/arr/err/check_nonzero_size.hpp>
#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <algorithm>
#include <limits>
#include <vector>

namespace stan {
namespace math {

/**
 * Returns the minimum coefficient in the specified
 * column vector.
 * @param x Specified vector.
 * @return Minimum coefficient value in the vector.
 * @tparam Type of values being compared and returned
 */
inline int min(const std::vector<int>& x) {
  check_nonzero_size("min", "int vector", x);
  Eigen::Map<const Eigen::Matrix<int, Eigen::Dynamic, 1>> m(&x[0], x.size());
  return m.minCoeff();
}

/**
 * Returns the minimum coefficient in the specified
 * column vector.
 * @param x Specified vector.
 * @return Minimum coefficient value in the vector.
 * @tparam Type of values being compared and returned
 */
template <typename T>
inline T min(const std::vector<T>& x) {
  if (x.size() == 0)
    return std::numeric_limits<T>::infinity();
  Eigen::Map<const Eigen::Matrix<T, Eigen::Dynamic, 1>> m(&x[0], x.size());
  return m.minCoeff();
}

/**
 * Returns the minimum coefficient in the specified
 * matrix, vector, or row vector.
 * @param m Specified matrix, vector, or row vector.
 * @return Minimum coefficient value in the vector.
 */
template <typename T, int R, int C>
inline T min(const Eigen::Matrix<T, R, C>& m) {
  if (m.size() == 0)
    return std::numeric_limits<double>::infinity();
  return m.minCoeff();
}

}  // namespace math
}  // namespace stan
#endif
