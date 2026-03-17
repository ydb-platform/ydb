#ifndef STAN_MATH_PRIM_ARR_FUN_FILL_HPP
#define STAN_MATH_PRIM_ARR_FUN_FILL_HPP

#include <stan/math/prim/scal/fun/fill.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Fill the specified container with the specified value.
 *
 * Each container in the specified standard vector is filled
 * recursively by calling <code>fill</code>.
 *
 * @tparam T Type of container in vector.
 * @tparam S Type of value.
 * @param[in] x Container.
 * @param[in, out] y Value.
 */
template <typename T, typename S>
void fill(std::vector<T>& x, const S& y) {
  for (typename std::vector<T>::size_type i = 0; i < x.size(); ++i)
    fill(x[i], y);
}

}  // namespace math
}  // namespace stan
#endif
