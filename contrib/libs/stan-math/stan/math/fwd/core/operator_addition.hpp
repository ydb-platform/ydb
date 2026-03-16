#ifndef STAN_MATH_FWD_CORE_OPERATOR_ADDITION_HPP
#define STAN_MATH_FWD_CORE_OPERATOR_ADDITION_HPP

#include <stan/math/fwd/core/fvar.hpp>

namespace stan {
namespace math {

/**
 * Return the sum of the specified forward mode addends.
 *
 * @tparam T type of values and tangents
 * @param x1 first addend
 * @param x2 second addend
 * @return sum of addends
 */
template <typename T>
inline fvar<T> operator+(const fvar<T>& x1, const fvar<T>& x2) {
  return fvar<T>(x1.val_ + x2.val_, x1.d_ + x2.d_);
}

/**
 * Return the sum of the specified double and forward mode addends.
 *
 * @tparam T type of values and tangents
 * @param x1 first addend
 * @param x2 second addend
 * @return sum of addends
 */
template <typename T>
inline fvar<T> operator+(double x1, const fvar<T>& x2) {
  return fvar<T>(x1 + x2.val_, x2.d_);
}

/**
 * Return the sum of the specified forward mode and double addends.
 *
 * @tparam T type of values and tangents
 * @param x1 first addend
 * @param x2 second addend
 * @return sum of addends
 */
template <typename T>
inline fvar<T> operator+(const fvar<T>& x1, double x2) {
  return fvar<T>(x1.val_ + x2, x1.d_);
}

}  // namespace math
}  // namespace stan
#endif
