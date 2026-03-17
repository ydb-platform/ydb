#ifndef STAN_MATH_PRIM_MAT_FUN_SORT_ASC_HPP
#define STAN_MATH_PRIM_MAT_FUN_SORT_ASC_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <algorithm>

namespace stan {
namespace math {

/**
 * Return the specified vector in ascending order.
 *
 * @tparam T Type of elements contained in vector.
 * @param xs Vector to order.
 * @return Vector in ascending order.
 * @throw std::domain_error If any of the values are NaN.
 */
template <typename T, int R, int C>
inline Eigen::Matrix<T, R, C> sort_asc(Eigen::Matrix<T, R, C> xs) {
  check_not_nan("sort_asc", "container argument", xs);
  std::sort(xs.data(), xs.data() + xs.size());
  return xs;
}

}  // namespace math
}  // namespace stan
#endif
