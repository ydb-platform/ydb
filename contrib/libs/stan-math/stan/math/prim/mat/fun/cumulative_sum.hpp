#ifndef STAN_MATH_PRIM_MAT_FUN_CUMULATIVE_SUM_HPP
#define STAN_MATH_PRIM_MAT_FUN_CUMULATIVE_SUM_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <vector>
#include <numeric>
#include <functional>

namespace stan {
namespace math {

/**
 * Return the cumulative sum of the specified vector.
 *
 * The cumulative sum of a vector of values \code{x} is the
 *
 * @code x[0], x[1] + x[2], ..., x[1] + , ..., + x[x.size()-1] @endcode
 *
 * @tparam T Scalar type of vector.
 * @param x Vector of values.
 * @return Cumulative sum of values.
 */
template <typename T>
inline std::vector<T> cumulative_sum(const std::vector<T>& x) {
  std::vector<T> result(x.size());
  if (x.size() == 0)
    return result;
  std::partial_sum(x.begin(), x.end(), result.begin(), std::plus<T>());
  return result;
}

/**
 * Return the cumulative sum of the specified matrix.
 *
 * The cumulative sum is of the same type as the input and
 * has values defined by
 *
 * @code x(0), x(1) + x(2), ..., x(1) + , ..., + x(x.size()-1) @endcode
 *
 * @tparam T Scalar type of matrix.
 * @tparam R Row type of matrix.
 * @tparam C Column type of matrix.
 * @param m Matrix of values.
 * @return Cumulative sum of values.
 */
template <typename T, int R, int C>
inline Eigen::Matrix<T, R, C> cumulative_sum(const Eigen::Matrix<T, R, C>& m) {
  Eigen::Matrix<T, R, C> result(m.rows(), m.cols());
  if (m.size() == 0)
    return result;
  std::partial_sum(m.data(), m.data() + m.size(), result.data(),
                   std::plus<T>());
  return result;
}
}  // namespace math
}  // namespace stan
#endif
