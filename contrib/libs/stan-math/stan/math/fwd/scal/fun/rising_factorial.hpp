#ifndef STAN_MATH_FWD_SCAL_FUN_RISING_FACTORIAL_HPP
#define STAN_MATH_FWD_SCAL_FUN_RISING_FACTORIAL_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/rising_factorial.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>

namespace stan {
namespace math {

/**
 * Return autodiff variable with the gradient and
 * result of the rising factorial function applied
 * to the inputs.
 * Will throw for NaN x and for negative n, as
 * implemented in primitive function.
 *
 * @tparam T Scalar type of autodiff variable.
 * @param x Argument.
 * @param n Argument
 * @return tangent of rising factorial at arguments.
 */

template <typename T>
inline fvar<T> rising_factorial(const fvar<T>& x, int n) {
  T rising_fact(rising_factorial(x.val_, n));
  return fvar<T>(rising_fact,
                 rising_fact * x.d_ * (digamma(x.val_ + n) - digamma(x.val_)));
}
}  // namespace math
}  // namespace stan
#endif
