#ifndef STAN_MATH_FWD_CORE_OPERATOR_DIVISION_HPP
#define STAN_MATH_FWD_CORE_OPERATOR_DIVISION_HPP

#include <stan/math/fwd/core/fvar.hpp>

namespace stan {
namespace math {

/**
 * Return the result of dividing the first argument by the second.
 *
 * @tparam T type of fvar value and tangent
 * @param x1 first argument
 * @param x2 second argument
 * @return first argument divided by second argument
 */
template <typename T>
inline fvar<T> operator/(const fvar<T>& x1, const fvar<T>& x2) {
  return fvar<T>(x1.val_ / x2.val_,
                 (x1.d_ * x2.val_ - x1.val_ * x2.d_) / (x2.val_ * x2.val_));
}

/**
 * Return the result of dividing the first argument by the second.
 *
 * @tparam T type of fvar value and tangent
 * @param x1 first argument
 * @param x2 second argument
 * @return first argument divided by second argument
 */
template <typename T>
inline fvar<T> operator/(const fvar<T>& x1, double x2) {
  return fvar<T>(x1.val_ / x2, x1.d_ / x2);
}

/**
 * Return the result of dividing the first argument by the second.
 *
 * @tparam T type of fvar value and tangent
 * @param x1 first argument
 * @param x2 second argument
 * @return first argument divided by second argument
 */
template <typename T>
inline fvar<T> operator/(double x1, const fvar<T>& x2) {
  // TODO(carpenter): store x1 / x2.val_ and reuse
  return fvar<T>(x1 / x2.val_, -x1 * x2.d_ / (x2.val_ * x2.val_));
}
}  // namespace math
}  // namespace stan
#endif
