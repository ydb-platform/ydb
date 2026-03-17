#ifndef STAN_MATH_PRIM_SCAL_FUN_IDENTITY_FREE_HPP
#define STAN_MATH_PRIM_SCAL_FUN_IDENTITY_FREE_HPP

namespace stan {
namespace math {

/**
 * Returns the result of applying the inverse of the identity
 * constraint transform to the input.
 *
 * <p>This function is a no-op and mainly useful as a placeholder
 * in auto-generated code.
 *
 * @tparam T type of value
 * @param[in] y value
 * @return value
 */
template <typename T>
inline T identity_free(const T& y) {
  return y;
}

}  // namespace math

}  // namespace stan

#endif
