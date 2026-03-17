#ifndef STAN_MATH_FWD_SCAL_FUN_LOG_FALLING_FACTORIAL_HPP
#define STAN_MATH_FWD_SCAL_FUN_LOG_FALLING_FACTORIAL_HPP

#include <stan/math/fwd/core.hpp>

#include <stan/math/prim/scal/fun/log_falling_factorial.hpp>
#include <boost/math/special_functions/digamma.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> log_falling_factorial(const fvar<T>& x, const fvar<T>& n) {
  using boost::math::digamma;

  return fvar<T>(log_falling_factorial(x.val_, n.val_),
                 (digamma(x.val_ + 1) - digamma(x.val_ - n.val_ + 1)) * x.d_
                     + digamma(x.val_ - n.val_ + 1) * n.d_);
}

template <typename T>
inline fvar<T> log_falling_factorial(double x, const fvar<T>& n) {
  using boost::math::digamma;

  return fvar<T>(log_falling_factorial(x, n.val_),
                 digamma(x - n.val_ + 1) * n.d_);
}

template <typename T>
inline fvar<T> log_falling_factorial(const fvar<T>& x, double n) {
  using boost::math::digamma;

  return fvar<T>(log_falling_factorial(x.val_, n),
                 (digamma(x.val_ + 1) - digamma(x.val_ - n + 1)) * x.d_);
}
}  // namespace math
}  // namespace stan
#endif
