#ifndef STAN_MATH_FWD_CORE_OPERATOR_LESS_THAN_HPP
#define STAN_MATH_FWD_CORE_OPERATOR_LESS_THAN_HPP

#include <stan/math/fwd/core/fvar.hpp>

namespace stan {
namespace math {

/**
 * Return true if the first argument has a value less than the
 * value of the second argument as defined by <code>&lt;</code>.
 *
 * @tparam value and tangent type for variables
 * @param[in] x first argument
 * @param[in] y second argument
 * @return true if the first argument's value is less than the
 * second argument's value
 */
template <typename T>
inline bool operator<(const fvar<T>& x, const fvar<T>& y) {
  return x.val_ < y.val_;
}

/**
 * Return true if the first argument is less than the value of the
 * second argument as defined by <code>&lt;</code>.
 *
 * @tparam value and tangent type for variables
 * @param[in] x first argument
 * @param[in] y second argument
 * @return true if the first argument is less than the second's
 * value argument
 */
template <typename T>
inline bool operator<(double x, const fvar<T>& y) {
  return x < y.val_;
}

/**
 * Return true if the first argument has a value less than the
 * second argument as defined by <code>&lt;</code>.
 *
 * @tparam value and tangent type for variables
 * @param[in] x first argument
 * @param[in] y second argument
 * @return true if the first argument has a value less than the
 * second argument
 * argument
 */
template <typename T>
inline bool operator<(const fvar<T>& x, double y) {
  return x.val_ < y;
}

}  // namespace math
}  // namespace stan
#endif
