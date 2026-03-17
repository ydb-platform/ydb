#ifndef STAN_MATH_PRIM_ARR_META_LENGTH_HPP
#define STAN_MATH_PRIM_ARR_META_LENGTH_HPP

#include <cstdlib>
#include <vector>

namespace stan {
/**
 * Returns the length of the provided std::vector.
 *
 * @param x input vector
 * @tparam T type of the elements in the vector
 * @return the length of the input vector
 */
template <typename T>
size_t length(const std::vector<T>& x) {
  return x.size();
}
}  // namespace stan
#endif
