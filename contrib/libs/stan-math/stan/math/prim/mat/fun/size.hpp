#ifndef STAN_MATH_PRIM_MAT_FUN_SIZE_HPP
#define STAN_MATH_PRIM_MAT_FUN_SIZE_HPP

#include <vector>

namespace stan {
namespace math {

/**
 * Return the size of the specified standard vector.
 *
 * @tparam T Type of elements.
 * @param[in] x Input vector.
 * @return Size of input vector.
 */
template <typename T>
inline int size(const std::vector<T>& x) {
  return static_cast<int>(x.size());
}

}  // namespace math
}  // namespace stan
#endif
