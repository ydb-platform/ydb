#ifndef STAN_MATH_FWD_CORE_OPERATOR_EQUAL_HPP
#define STAN_MATH_FWD_CORE_OPERATOR_EQUAL_HPP

#include <stan/math/fwd/core/fvar.hpp>

namespace stan {
namespace math {

/**
 * Return true if the specified variables have equal values as
 * defined by <code>==</code>.
 *
 * @tparam value and tangent type for variables
 * @param[in] x first argument
 * @param[in] y second argument
 * @return true if the arguments have equal values
 */
template <typename T>
inline bool operator==(const fvar<T>& x, const fvar<T>& y) {
  return x.val_ == y.val_;
}

/**
 * Return true if the the first variable has a value equal to
 * the second argument as defined by
 * by <code>==</code>.
 *
 * @tparam value and tangent type for variables
 * @param[in] x first argument
 * @param[in] y second argument
 * @return true if the arguments have equal values
 */
template <typename T>
inline bool operator==(const fvar<T>& x, double y) {
  return x.val_ == y;
}

/**
 * Return true if the the first argument is equal to the value of
 * the second argument as defined by by <code>==</code>.
 *
 * @tparam value and tangent type for variables
 * @param[in] x first argument
 * @param[in] y second argument
 * @return true if the arguments have equal values
 */
template <typename T>
inline bool operator==(double x, const fvar<T>& y) {
  return x == y.val_;
}

}  // namespace math
}  // namespace stan
#endif
