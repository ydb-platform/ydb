#ifndef STAN_MATH_PRIM_ARR_FUN_SUM_HPP
#define STAN_MATH_PRIM_ARR_FUN_SUM_HPP

#include <cstddef>
#include <vector>
#include <numeric>

namespace stan {
namespace math {

/**
 * Return the sum of the values in the specified standard vector.
 *
 * @tparam T Type of elements summed.
 * @param xs Standard vector to sum.
 * @return Sum of elements.
 */
template <typename T>
inline T sum(const std::vector<T>& xs) {
  return std::accumulate(xs.begin(), xs.end(), T{0});
}

}  // namespace math
}  // namespace stan
#endif
