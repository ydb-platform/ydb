#ifndef STAN_MATH_FWD_CORE_OPERATOR_GREATER_THAN_HPP
#define STAN_MATH_FWD_CORE_OPERATOR_GREATER_THAN_HPP

#include <stan/math/fwd/core/fvar.hpp>

namespace stan {
namespace math {

/**
 * Return true if the first argument has a greater value than the
 * second as defined by <code>&gt;</code>.
 *
 * @tparam value and tangent type for variables
 * @param[in] x first argument
 * @param[in] y second argument
 * @return true if the first argument has a greater value than the
 * second
 */
template <typename T>
inline bool operator>(const fvar<T>& x, const fvar<T>& y) {
  return x.val_ > y.val_;
}

/**
 * Return true if the first argument has a greater value than the
 * second as defined by <code>&gt;</code>.
 *
 * @tparam value and tangent type for variables
 * @param[in] x first argument
 * @param[in] y second argument
 * @return true if the first argument has a greater value than the
 * second
 */
template <typename T>
inline bool operator>(const fvar<T>& x, double y) {
  return x.val_ > y;
}

/**
 * Return true if the first argument has a greater value than the
 * second as defined by <code>&gt;</code>.
 *
 * @tparam value and tangent type for variables
 * @param[in] x first argument
 * @param[in] y second argument
 * @return true if the first argument has a greater value than the
 * second
 */
template <typename T>
inline bool operator>(double x, const fvar<T>& y) {
  return x > y.val_;
}

}  // namespace math
}  // namespace stan
#endif
