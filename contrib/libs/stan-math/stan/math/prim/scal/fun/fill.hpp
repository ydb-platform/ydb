#ifndef STAN_MATH_PRIM_SCAL_FUN_FILL_HPP
#define STAN_MATH_PRIM_SCAL_FUN_FILL_HPP

namespace stan {
namespace math {

/**
 * Fill the specified container with the specified value.
 *
 * This base case simply assigns the value to the container.
 *
 * @tparam T Type of reference container.
 * @tparam S Type of value.
 * @param x Container.
 * @param y Value.
 */
template <typename T, typename S>
void fill(T& x, const S& y) {
  x = y;
}

}  // namespace math
}  // namespace stan
#endif
