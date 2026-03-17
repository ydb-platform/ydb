#ifndef STAN_MATH_PRIM_ARR_FUN_SORT_ASC_HPP
#define STAN_MATH_PRIM_ARR_FUN_SORT_ASC_HPP

#include <stan/math/prim/scal/err/check_not_nan.hpp>
#include <algorithm>
#include <vector>

namespace stan {
namespace math {

/**
 * Return the specified standard vector in ascending order.
 *
 * @tparam T Type of elements contained in vector.
 * @param xs Vector to order.
 * @return Vector in ascending order.
 * @throw std::domain_error If any of the values are NaN.
 */
template <typename T>
inline std::vector<T> sort_asc(std::vector<T> xs) {
  check_not_nan("sort_asc", "container argument", xs);
  std::sort(xs.begin(), xs.end());
  return xs;
}

}  // namespace math
}  // namespace stan
#endif
