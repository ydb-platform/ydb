#ifndef STAN_MATH_FWD_SCAL_FUN_FMIN_HPP
#define STAN_MATH_FWD_SCAL_FUN_FMIN_HPP

#include <stan/math/fwd/core.hpp>
#include <stan/math/prim/scal/fun/constants.hpp>
#include <stan/math/prim/scal/fun/fmin.hpp>
#include <stan/math/prim/scal/fun/is_nan.hpp>
#include <stan/math/prim/scal/meta/likely.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> fmin(const fvar<T>& x1, const fvar<T>& x2) {
  if (unlikely(is_nan(x1.val_))) {
    if (is_nan(x2.val_))
      return fvar<T>(fmin(x1.val_, x2.val_), NOT_A_NUMBER);
    else
      return fvar<T>(x2.val_, x2.d_);
  } else if (unlikely(is_nan(x2.val_))) {
    return fvar<T>(x1.val_, x1.d_);
  } else if (x1.val_ < x2.val_) {
    return fvar<T>(x1.val_, x1.d_);
  } else if (x1.val_ == x2.val_) {
    return fvar<T>(x1.val_, NOT_A_NUMBER);
  } else {
    return fvar<T>(x2.val_, x2.d_);
  }
}

template <typename T>
inline fvar<T> fmin(double x1, const fvar<T>& x2) {
  if (unlikely(is_nan(x1))) {
    if (is_nan(x2.val_))
      return fvar<T>(fmin(x1, x2.val_), NOT_A_NUMBER);
    else
      return fvar<T>(x2.val_, x2.d_);
  } else if (unlikely(is_nan(x2.val_))) {
    return fvar<T>(x1, 0.0);
  } else if (x1 < x2.val_) {
    return fvar<T>(x1, 0.0);
  } else if (x1 == x2.val_) {
    return fvar<T>(x2.val_, NOT_A_NUMBER);
  } else {
    return fvar<T>(x2.val_, x2.d_);
  }
}

template <typename T>
inline fvar<T> fmin(const fvar<T>& x1, double x2) {
  if (unlikely(is_nan(x1.val_))) {
    if (is_nan(x2))
      return fvar<T>(fmin(x1.val_, x2), NOT_A_NUMBER);
    else
      return fvar<T>(x2, 0.0);
  } else if (unlikely(is_nan(x2))) {
    return fvar<T>(x1.val_, x1.d_);
  } else if (x1.val_ < x2) {
    return fvar<T>(x1.val_, x1.d_);
  } else if (x1.val_ == x2) {
    return fvar<T>(x1.val_, NOT_A_NUMBER);
  } else {
    return fvar<T>(x2, 0.0);
  }
}

}  // namespace math
}  // namespace stan
#endif
