#ifndef STAN_MATH_PRIM_ARR_ERR_IS_NONZERO_SIZE_HPP
#define STAN_MATH_PRIM_ARR_ERR_IS_NONZERO_SIZE_HPP

namespace stan {
namespace math {

/**
 * Returns <code>true</code> if the specified matrix/vector is size nonzero.
 * @tparam T_y Type of container, requires class method <code>.size()</code>
 * @param y Container to test -- matrix/vector
 * @return <code>true</code> if container has size zero
 */
template <typename T_y>
inline bool is_nonzero_size(const T_y& y) {
  return y.size() > 0;
}

}  // namespace math
}  // namespace stan
#endif
