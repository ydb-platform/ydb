#ifndef STAN_MATH_PRIM_SCAL_FUN_IS_UNINITIALIZED_HPP
#define STAN_MATH_PRIM_SCAL_FUN_IS_UNINITIALIZED_HPP

namespace stan {
namespace math {

/**
 * Returns <code>true</code> if the specified variable is
 * uninitialized.  Arithmetic types are always initialized
 * by definition (the value is not specified).
 *
 * @tparam T Type of object to test.
 * @param x Object to test.
 * @return <code>true</code> if the specified object is uninitialized.
 * @return false if input is NaN.
 */
template <typename T>
inline bool is_uninitialized(T x) {
  return false;
}

}  // namespace math
}  // namespace stan
#endif
