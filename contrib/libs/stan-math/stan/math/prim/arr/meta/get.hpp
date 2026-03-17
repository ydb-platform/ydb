#ifndef STAN_MATH_PRIM_ARR_META_GET_HPP
#define STAN_MATH_PRIM_ARR_META_GET_HPP

#include <cstdlib>
#include <vector>

namespace stan {
/**
 * Returns the n-th element of the provided std::vector.
 *
 * @param x input vector
 * @param n index of the element to return
 * @return n-th element of the input vector
 */
template <typename T>
inline T get(const std::vector<T>& x, size_t n) {
  return x[n];
}

}  // namespace stan
#endif
