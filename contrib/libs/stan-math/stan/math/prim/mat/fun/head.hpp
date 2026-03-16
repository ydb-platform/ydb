#ifndef STAN_MATH_PRIM_MAT_FUN_HEAD_HPP
#define STAN_MATH_PRIM_MAT_FUN_HEAD_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/err/check_column_index.hpp>
#include <stan/math/prim/mat/err/check_row_index.hpp>
#include <stan/math/prim/mat/err/check_std_vector_index.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Return the specified number of elements as a vector
 * from the front of the specified vector.
 *
 * @tparam T Type of value in vector.
 * @param v Vector input.
 * @param n Size of return.
 * @return The first n elements of v.
 * @throw std::out_of_range if n is out of range.
 */
template <typename T>
inline Eigen::Matrix<T, Eigen::Dynamic, 1> head(
    const Eigen::Matrix<T, Eigen::Dynamic, 1>& v, size_t n) {
  if (n != 0)
    check_row_index("head", "n", v, n);
  return v.head(n);
}

/**
 * Return the specified number of elements as a row vector
 * from the front of the specified row vector.
 *
 * @tparam T Type of value in vector.
 * @param rv Row vector.
 * @param n Size of return row vector.
 * @return The first n elements of rv.
 * @throw std::out_of_range if n is out of range.
 */
template <typename T>
inline Eigen::Matrix<T, 1, Eigen::Dynamic> head(
    const Eigen::Matrix<T, 1, Eigen::Dynamic>& rv, size_t n) {
  if (n != 0)
    check_column_index("head", "n", rv, n);
  return rv.head(n);
}

/**
 * Return the specified number of elements as a standard vector
 * from the front of the specified standard vector.
 *
 * @tparam T Type of value in vector.
 * @param sv Standard vector.
 * @param n Size of return.
 * @return The first n elements of sv.
 * @throw std::out_of_range if n is out of range.
 */
template <typename T>
std::vector<T> head(const std::vector<T>& sv, size_t n) {
  if (n != 0)
    check_std_vector_index("head", "n", sv, n);

  std::vector<T> s;
  for (size_t i = 0; i < n; ++i)
    s.push_back(sv[i]);
  return s;
}

}  // namespace math
}  // namespace stan
#endif
