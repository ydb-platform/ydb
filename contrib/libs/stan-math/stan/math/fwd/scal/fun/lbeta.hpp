#ifndef STAN_MATH_FWD_SCAL_FUN_LBETA_HPP
#define STAN_MATH_FWD_SCAL_FUN_LBETA_HPP

#include <stan/math/fwd/core.hpp>

#include <boost/math/special_functions/digamma.hpp>
#include <stan/math/prim/scal/fun/lbeta.hpp>

namespace stan {
namespace math {

template <typename T>
inline fvar<T> lbeta(const fvar<T>& x1, const fvar<T>& x2) {
  using boost::math::digamma;
  return fvar<T>(lbeta(x1.val_, x2.val_),
                 x1.d_ * digamma(x1.val_) + x2.d_ * digamma(x2.val_)
                     - (x1.d_ + x2.d_) * digamma(x1.val_ + x2.val_));
}

template <typename T>
inline fvar<T> lbeta(double x1, const fvar<T>& x2) {
  using boost::math::digamma;
  return fvar<T>(lbeta(x1, x2.val_),
                 x2.d_ * digamma(x2.val_) - x2.d_ * digamma(x1 + x2.val_));
}

template <typename T>
inline fvar<T> lbeta(const fvar<T>& x1, double x2) {
  using boost::math::digamma;
  return fvar<T>(lbeta(x1.val_, x2),
                 x1.d_ * digamma(x1.val_) - x1.d_ * digamma(x1.val_ + x2));
}
}  // namespace math
}  // namespace stan
#endif
