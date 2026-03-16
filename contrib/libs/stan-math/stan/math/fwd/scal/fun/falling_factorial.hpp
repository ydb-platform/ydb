#ifndef STAN_MATH_FWD_SCAL_FUN_FALLING_FACTORIAL_HPP
#define STAN_MATH_FWD_SCAL_FUN_FALLING_FACTORIAL_HPP

#include <stan/math/fwd/core.hpp>

#include <stan/math/prim/scal/fun/falling_factorial.hpp>
#include <stan/math/prim/scal/fun/digamma.hpp>

namespace stan {
namespace math {

/**
 * Return autodiff variable with the gradient and
 * result of the falling factorial function applied
 * to the inputs.
 * Will throw for NaN x and for negative n, as
 * implemented in primitive function.
 *
 * @tparam T Scalar type of autodiff variable.
 * @param x Argument.
 * @param n Argument
 * @return tangent of falling factorial at arguments.
 */

template <typename T>
inline fvar<T> falling_factorial(const fvar<T>& x, int n) {
  T falling_fact(falling_factorial(x.val_, n));
  return fvar<T>(
      falling_fact,
      falling_fact * (digamma(x.val_ + 1) - digamma(x.val_ - n + 1)) * x.d_);
}
}  // namespace math
}  // namespace stan
#endif
