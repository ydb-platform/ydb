#ifndef STAN_MATH_FWD_CORE_OPERATOR_GREATER_THAN_OR_EQUAL_HPP
#define STAN_MATH_FWD_CORE_OPERATOR_GREATER_THAN_OR_EQUAL_HPP

#include <stan/math/fwd/core/fvar.hpp>

namespace stan {
namespace math {

/**
 * Return true if the value of the first argument is greater than or
 * equal to that of the second as defined by <code>&gt;=</code>.
 *
 * @tparam value and tangent type for variables
 * @param[in] x first argument
 * @param[in] y second argument
 * @return true if the first argument has a value greater than or
 * equal to that of the second
 */
template <typename T>
inline bool operator>=(const fvar<T>& x, const fvar<T>& y) {
  return x.val_ >= y.val_;
}

/**
 * Return true if the value of the first argument has a value
 * greater than or equal to the second argument as defined by
 * <code>&gt;=</code>.
 *
 * @tparam value and tangent type for variables
 * @param[in] x first argument
 * @param[in] y second argument
 * @return true if the first argument has a value greater than or
 * equal to that of the second
 */
template <typename T>
inline bool operator>=(const fvar<T>& x, double y) {
  return x.val_ >= y;
}

/**
 * Return true if the first argument is greater than or equal to
 * the value of the second argument as defined by
 * <code>&gt;=</code>.
 *
 * @tparam value and tangent type for variables
 * @param[in] x first argument
 * @param[in] y second argument
 * @return true if the first argument has a value greater than or
 * equal to that of the second
 */
template <typename T>
inline bool operator>=(double x, const fvar<T>& y) {
  return x >= y.val_;
}

}  // namespace math
}  // namespace stan
#endif
