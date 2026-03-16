#ifndef STAN_MATH_FWD_SCAL_FUN_ATAN2_HPP
#define STAN_MATH_FWD_SCAL_FUN_ATAN2_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/square.hpp>
#include <cmath>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> atan2(const fvar<T>& x1, const fvar<T>& x2) {
  using std::atan2;
  return fvar<T>(atan2(x1.val_, x2.val_),
                 (x1.d_ * x2.val_ - x1.val_ * x2.d_)
                     / (square(x2.val_) + square(x1.val_)));
}

template <typename T>
inline fvar<T> atan2(double x1, const fvar<T>& x2) {
  using std::atan2;
  return fvar<T>(atan2(x1, x2.val_),
                 (-x1 * x2.d_) / (square(x1) + square(x2.val_)));
}

template <typename T>
inline fvar<T> atan2(const fvar<T>& x1, double x2) {
  using std::atan2;
  return fvar<T>(atan2(x1.val_, x2),
                 (x1.d_ * x2) / (square(x2) + square(x1.val_)));
}

}  // namespace math
}  // namespace stan
#endif
