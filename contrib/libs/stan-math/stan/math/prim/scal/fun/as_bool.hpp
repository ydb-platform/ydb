#ifndef STAN_MATH_PRIM_SCAL_FUN_AS_BOOL_HPP
#define STAN_MATH_PRIM_SCAL_FUN_AS_BOOL_HPP

namespace stan {
namespace math {

/**
 * Return true if the argument is not equal to zero (in the
 * <code>!=</code> operator sense) and false otherwise.
 *
 * @tparam T type of scalar
 * @param x value
 * @return true if value is not equal to zero
 */
template <typename T>
inline bool as_bool(const T& x) {
  return x != 0;
}

}  // namespace math
}  // namespace stan

#endif
