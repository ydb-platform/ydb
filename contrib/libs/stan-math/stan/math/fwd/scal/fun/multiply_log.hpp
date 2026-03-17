#ifndef STAN_MATH_FWD_SCAL_FUN_MULTIPLY_LOG_HPP
#define STAN_MATH_FWD_SCAL_FUN_MULTIPLY_LOG_HPP

#include <stan/math/fwd/core.hpp>

#include <stan/math/prim/scal/fun/multiply_log.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> multiply_log(const fvar<T>& x1, const fvar<T>& x2) {
  using std::log;
  return fvar<T>(multiply_log(x1.val_, x2.val_),
                 x1.d_ * log(x2.val_) + x1.val_ * x2.d_ / x2.val_);
}

template <typename T>
inline fvar<T> multiply_log(double x1, const fvar<T>& x2) {
  using std::log;
  return fvar<T>(multiply_log(x1, x2.val_), x1 * x2.d_ / x2.val_);
}

template <typename T>
inline fvar<T> multiply_log(const fvar<T>& x1, double x2) {
  using std::log;
  return fvar<T>(multiply_log(x1.val_, x2), x1.d_ * log(x2));
}
}  // namespace math
}  // namespace stan
#endif
